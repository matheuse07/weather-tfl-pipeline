import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def create_spark_session():
    return (
        SparkSession.builder.appName("TFL Bronze to Silver")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


# Expected schema for line status records
LINE_STATUS_SCHEMA = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("lineStatuses", StringType(), True),  # JSON string, exploded below
        StructField("ingested_at", StringType(), True),
    ]
)


def process_line_status(spark: SparkSession, partition_date: str):
    bronze_path = f"s3a://tfl-weather-bronze/tfl/line_status/year={partition_date[:4]}/month={partition_date[5:7]}/day={partition_date[8:10]}/**/*.json"

    # Read raw JSON (multiline = each file is one JSON object with a records array)
    raw_df = spark.read.option("multiline", "true").json(bronze_path)

    # Explode the records array into individual rows
    exploded_df = raw_df.select(
        F.col("ingested_at"), F.explode(F.col("records")).alias("record")
    )

    # Flatten and select the fields we need
    flat_df = exploded_df.select(
        F.col("record.id").alias("line_id"),
        F.col("record.name").alias("line_name"),
        F.col("record.modeName").alias("mode_name"),
        F.explode(F.col("record.lineStatuses")).alias("status"),
        F.to_timestamp(F.col("ingested_at")).alias("ingested_at"),
    ).select(
        "line_id",
        "line_name",
        "mode_name",
        F.col("status.id").alias("status_id"),
        F.col("status.statusSeverity").cast(IntegerType()).alias("severity"),
        F.col("status.statusSeverityDescription").alias("severity_description"),
        F.col("status.reason").alias("reason"),
        "ingested_at",
    )

    # Data quality: drop rows with null line_id or severity
    clean_df = flat_df.filter(
        F.col("line_id").isNotNull() & F.col("severity").isNotNull()
    )

    # Add derived columns useful for ML
    enriched_df = clean_df.withColumn(
        "is_disrupted", F.when(F.col("severity") < 10, True).otherwise(False)
    ).withColumn("partition_date", F.lit(partition_date))

    # Write to Silver as Parquet, partitioned by date
    (
        enriched_df.repartition(1)  # small dataset, 1 file per partition is fine
        .write.mode("overwrite")
        .partitionBy("partition_date")
        .parquet("s3a://tfl-weather-silver/tfl_line_status/")
    )

    count = enriched_df.count()
    print(f"Wrote {count} rows to Silver tfl_line_status for {partition_date}")
    return count


def main():
    partition_date = sys.argv[1]  # e.g. "2024-06-15"
    spark = create_spark_session()

    try:
        process_line_status(spark, partition_date)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
