"""
extract.py
Handles extraction of pharmacy claims data from AWS S3.
Supports full load and incremental (date-partitioned) extraction.
"""

import boto3
import pandas as pd
from datetime import datetime, timedelta
from src.utils.logger import get_logger

logger = get_logger(__name__)


class ClaimsExtractor:
    """Extracts pharmacy claims data from AWS S3 data lake."""

    def __init__(self, config: dict):
        self.bucket = config["aws"]["s3_bucket"]
        self.raw_prefix = config["aws"]["raw_prefix"]
        self.region = config["aws"]["region"]
        self.s3_client = boto3.client("s3", region_name=self.region)

    def get_s3_keys(self, date: datetime) -> list[str]:
        """
        List all S3 keys for a given processing date.
        Files are partitioned as: raw/claims/year=YYYY/month=MM/day=DD/
        """
        prefix = (
            f"{self.raw_prefix}"
            f"year={date.year}/"
            f"month={date.month:02d}/"
            f"day={date.day:02d}/"
        )
        logger.info(f"Scanning S3 prefix: s3://{self.bucket}/{prefix}")

        paginator = self.s3_client.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    keys.append(obj["Key"])

        logger.info(f"Found {len(keys)} files for date {date.date()}")
        return keys

    def extract_claims(self, date: datetime) -> pd.DataFrame:
        """
        Extract all claims files for a given date from S3.
        Returns a consolidated Pandas DataFrame.
        """
        keys = self.get_s3_keys(date)

        if not keys:
            logger.warning(f"No files found for {date.date()} — skipping.")
            return pd.DataFrame()

        frames = []
        for key in keys:
            logger.info(f"Reading s3://{self.bucket}/{key}")
            obj = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            df = pd.read_parquet(obj["Body"])
            frames.append(df)

        combined = pd.concat(frames, ignore_index=True)
        logger.info(
            f"Extracted {len(combined):,} records from {len(keys)} files "
            f"for date {date.date()}"
        )
        return combined

    def extract_incremental(self, lookback_days: int = 1) -> pd.DataFrame:
        """
        Extract data for the last N days (default: yesterday).
        Used for daily incremental pipeline runs.
        """
        target_date = datetime.utcnow() - timedelta(days=lookback_days)
        logger.info(f"Starting incremental extract for {target_date.date()}")
        return self.extract_claims(date=target_date)

    def extract_full(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """
        Extract data for a date range (used for backfills).
        """
        logger.info(f"Starting full extract from {start_date.date()} to {end_date.date()}")
        all_frames = []
        current = start_date

        while current <= end_date:
            df = self.extract_claims(date=current)
            if not df.empty:
                all_frames.append(df)
            current += timedelta(days=1)

        if not all_frames:
            return pd.DataFrame()

        result = pd.concat(all_frames, ignore_index=True)
        logger.info(f"Full extract complete: {len(result):,} total records")
        return result
