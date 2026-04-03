import json
import logging
import os
from datetime import UTC, datetime

import boto3
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WeatherClient:
    FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
    ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

    LONDON_LAT = 51.5074
    LONDON_LON = -0.1278

    HOURLY_VARS = [
        "temperature_2m",
        "precipitation",
        "wind_speed_10m",
        "visibility",
        "weather_code",
        "cloud_cover",
        "relative_humidity_2m",
    ]

    def __init__(self):
        self.s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("S3_ENDPOINT_URL"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "eu-west-2"),
        )
        self.bucket = os.getenv("BRONZE_BUCKET")

    def fetch_forecast(self) -> dict:
        resp = requests.get(
            self.FORECAST_URL,
            params={
                "latitude": self.LONDON_LAT,
                "longitude": self.LONDON_LON,
                "hourly": ",".join(self.HOURLY_VARS),
                "timezone": "Europe/London",
                "forecast_days": 2,
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def fetch_historical(self, start_date: str, end_date: str) -> dict:
        """Fetch historical data for ML training. Dates: YYYY-MM-DD format."""
        response = requests.get(
            self.ARCHIVE_URL,
            params={
                "latitude": self.LONDON_LAT,
                "longitude": self.LONDON_LON,
                "start_date": start_date,
                "end_date": end_date,
                "hourly": ",".join(self.HOURLY_VARS),
                "timezone": "Europe/London",
            },
            timeout=60,
        )
        response.raise_for_status()
        return response.json()

    def upload_to_bronze(self, data: dict, execution_ts: datetime) -> str:
        ts = execution_ts.astimezone(UTC)
        key = (
            f"weather/forecast/"
            f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/hour={ts.hour:02d}/"
            f"{ts.strftime('%Y%m%d_%H%M%S')}.json"
        )
        payload = {
            "ingested_at": ts.isoformat(),
            "source": "open_meteo",
            "records": data,
        }
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(payload, default=str),
            ContentType="application/json",
        )
        logger.info(f"Uploaded weather forecast to s3://{self.bucket}/{key}")
        return key


if __name__ == "__main__":
    client = WeatherClient()
    forecast_data = client.fetch_forecast()
    print(json.dumps(forecast_data, indent=2))
