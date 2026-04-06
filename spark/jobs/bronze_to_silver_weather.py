import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def create_spark_session():
    return (
        SparkSession.builder.appName("Weather Bronze to Silver")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def process_weather(spark: SparkSession, partition_date: str):
    bronze_path = f"s3a://tfl-weather-bronze/weather/forecast/year={partition_date[:4]}/month={partition_date[5:7]}/day={partition_date[8:10]}/**/*.json"

    raw_df = spark.read.option("multiline", "true").json(bronze_path)

    # Open-Meteo returns parallel arrays for each hour — zip them together
    hourly_df = raw_df.select(
        F.col("ingested_at"),
        F.col("records.hourly.time").alias("times"),
        F.col("records.hourly.temperature_2m").alias("temps"),
        F.col("records.hourly.precipitation").alias("precip"),
        F.col("records.hourly.wind_speed_10m").alias("wind"),
        F.col("records.hourly.visibility").alias("visibility"),
        F.col("records.hourly.weather_code").alias("weather_code"),
        F.col("records.hourly.cloud_cover").alias("cloud_cover"),
    )

    # Use arrays_zip to combine parallel arrays into structs, then explode
    zipped_df = hourly_df.withColumn(
        "hourly",
        F.explode(
            F.arrays_zip(
                "times",
                "temps",
                "precip",
                "wind",
                "visibility",
                "weather_code",
                "cloud_cover",
            )
        ),
    ).select(
        F.to_timestamp(F.col("hourly.times"), "yyyy-MM-dd'T'HH:mm").alias(
            "observation_time"
        ),
        F.col("hourly.temps").alias("temperature_c"),
        F.col("hourly.precip").alias("precipitation_mm"),
        F.col("hourly.wind").alias("wind_speed_kmh"),
        F.col("hourly.visibility").alias("visibility_m"),
        F.col("hourly.weather_code").alias("weather_code"),
        F.col("hourly.cloud_cover").alias("cloud_cover_pct"),
        F.to_timestamp(F.col("ingested_at")).alias("ingested_at"),
    )

    # Add derived columns for ML features
    enriched_df = (
        zipped_df.withColumn("hour_of_day", F.hour("observation_time"))
        .withColumn("day_of_week", F.dayofweek("observation_time"))
        .withColumn(
            "is_peak_hour",
            F.when(
                (F.col("hour_of_day").between(7, 9))
                | (F.col("hour_of_day").between(17, 19)),
                True,
            ).otherwise(False),
        )
        .withColumn(
            "is_heavy_rain",
            F.when(F.col("precipitation_mm") > 5, True).otherwise(False),
        )
        .withColumn("partition_date", F.lit(partition_date))
    )

    # Data quality: filter out future forecasts (keep only past + current hour)
    # This ensures Silver only has observed data, not predictions
    clean_df = enriched_df.filter(F.col("observation_time") <= F.current_timestamp())

    (
        clean_df.repartition(1)
        .write.mode("overwrite")
        .partitionBy("partition_date")
        .parquet("s3a://tfl-weather-silver/weather_hourly/")
    )

    count = clean_df.count()
    print(f"Wrote {count} weather rows to Silver for {partition_date}")
    return count


def main():
    partition_date = sys.argv[1]
    spark = create_spark_session()
    try:
        process_weather(spark, partition_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
