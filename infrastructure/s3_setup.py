import boto3
import os
import logging

logging.basicConfig(level=logging.INFO)

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:4566"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    region_name="eu-west-2",
)

for bucket in ["tfl-weather-bronze", "tfl-weather-silver", "tfl-weather-gold"]:
    try:
        s3.create_bucket(
            Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
        )
        logging.info(f"Bucket '{bucket}' created successfully.")
    except s3.exceptions.BucketAlreadyOwnedByYou as e:
        logging.error(f"Bucket already exists - '{bucket}': {e}")
