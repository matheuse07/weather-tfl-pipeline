import json
import logging
import os
from datetime import UTC, datetime

import boto3
import requests

logging.basicConfig(level=logging.INFO)


class TFLClient:
    BASE_URL = "https://api.tfl.gov.uk"

    def __init__(self):
        self.app_key = os.getenv("TFL_APP_KEY", "test")
        self.s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:4566"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            region_name="eu-west-2",
        )
        self.bucket = os.getenv("BRONZE_BUCKET")

    def _get(self, path: str) -> dict | list:
        """Helper method to make GET requests to the TfL API."""
        url = f"{self.BASE_URL}{path}"
        params = {"app_key": self.app_key}
        response = requests.get(url, params=params)
        response.raise_for_status()

        return response.json()

    def fetch_line_status(self) -> list:
        """Fetches the current status of all TfL lines."""
        return self._get("/Line/Mode/tube/Status")

    def fetch_disruptions(self) -> list:
        """Fetches current disruptions across all TfL modes."""
        return self._get("/Line/Mode/tube/Disruption")

    def upload_to_bronze(
        self, data: list, data_type: str, execution_ts: datetime
    ) -> str:
        """
        Uploads raw JSON to S3 with Hive-style partitioning:
        tfl/{data_type}/year=YYYY/month=MM/day=DD/hour=HH/{timestamp}.json
        """
        ts = execution_ts.astimezone(UTC)
        key = (
            f"tfl/{data_type}/"
            f"year={ts.year}/month={ts.month:02d}/day={ts.day:02d}/hour={ts.hour:02d}/"
            f"{ts.strftime('%Y%m%d_%H%M%S')}.json"
        )
        payload = {
            "ingested_at": ts.isoformat(),
            "source": "tfl_api",
            "data_type": data_type,
            "record_count": len(data),
            "records": data,
        }
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(payload, default=str),
            ContentType="application/json",
        )
        print(f"Uploaded {len(data)} records to s3://{self.bucket}/{key}")
        return key

