"""
transform.py
PySpark transformation layer for pharmacy claims data.
Handles cleaning, standardization, HIPAA compliance,
and building the star schema dimensional model.
"""

import os
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from src.utils.logger import get_logger

logger = get_logger(__name__)

PYTHON_PATH = r"C:\Users\saira\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_PYTHON"]        = PYTHON_PATH
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_PATH


def get_spark_session(app_name: str = "HealthcareClaimsETL") -> SparkSession:
    """
    Create or retrieve a SparkSession with optimized configs.
    SparkSession is the entry point to all PySpark functionality.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .getOrCreate()
    )


class ClaimsTransformer:
    """
    Applies all PySpark transformations to raw claims data.
    Produces a star schema with 1 fact table and 3 dimension tables.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def run(self, raw_df: pd.DataFrame) -> dict:
        """
        Run the full transformation pipeline.
        Returns dict of Pandas DataFrames keyed by table name.
        """
        logger.info(f"Starting transformation - input: {len(raw_df):,} rows")

        sdf = self.spark.createDataFrame(raw_df)
        self.spark.sparkContext.setLogLevel("ERROR")
        logger.info("Spark DataFrame created")

        cleaned = self._clean(sdf)

        logger.info("Building dimension tables...")
        dim_drug     = self._build_dim_drug(cleaned)
        dim_provider = self._build_dim_provider(cleaned)
        dim_date     = self._build_dim_date(cleaned)

        logger.info("Building fact table...")
        fact_claims = self._build_fact_claims(cleaned)

        logger.info("Converting Spark DataFrames to Pandas...")
        result = {
            "dim_drug":     dim_drug.toPandas(),
            "dim_provider": dim_provider.toPandas(),
            "dim_date":     dim_date.toPandas(),
            "fact_claims":  fact_claims.toPandas(),
        }

        for name, df in result.items():
            logger.info(f"  {name}: {len(df):,} rows")

        return result

    def _clean(self, sdf: DataFrame) -> DataFrame:
        """
        Apply all cleaning and standardization rules.
        Raw messy data becomes clean analytics-ready data.
        """
        logger.info("Applying cleaning transformations...")

        return (
            sdf
            .dropDuplicates(["claim_id"])
            .withColumn("claim_amount", F.col("claim_amount").cast("double"))
            .withColumn("quantity",     F.col("quantity").cast("double"))
            .withColumn("days_supply",  F.col("days_supply").cast("integer"))
            .withColumn("service_date",
                        F.to_date(F.col("service_date"), "yyyy-MM-dd"))
            .withColumn("drug_class",
                        F.upper(F.trim(F.col("drug_class"))))
            .withColumn("claim_status",
                        F.upper(F.trim(F.col("claim_status"))))
            # HIPAA compliance - hash member IDs
            # SHA2 is one-way - cannot be reversed to original ID
            .withColumn("member_id",
                        F.sha2(F.col("member_id"), 256))
            .withColumn("etl_processed_at", F.current_timestamp())
            .filter(F.col("claim_amount") > 0)
            .filter(F.col("quantity") > 0)
            .filter(F.col("days_supply").between(1, 365))
        )

    def _build_dim_drug(self, df: DataFrame) -> DataFrame:
        """
        Build DIM_DRUG - one row per unique drug (15 rows).
        Small lookup table referenced by fact_claims.
        """
        return (
            df
            .select("ndc_code", "drug_name", "drug_class")
            .dropDuplicates(["ndc_code"])
            .withColumn("drug_key",
                        F.sha2(F.col("ndc_code").cast("string"), 256))
        )

    def _build_dim_provider(self, df: DataFrame) -> DataFrame:
        """
        Build DIM_PROVIDER - one row per unique provider.
        NPI = National Provider Identifier, unique ID for US healthcare providers.
        """
        return (
            df
            .select("npi_number", "member_state")
            .dropDuplicates(["npi_number"])
            .withColumn("provider_key",
                        F.sha2(F.col("npi_number").cast("string"), 256))
        )

    def _build_dim_date(self, df: DataFrame) -> DataFrame:
        """
        Build DIM_DATE - one row per unique date (336 rows).
        Extracts year, month, quarter for easy time-based filtering.
        """
        return (
            df
            .select(F.col("service_date").alias("date_value"))
            .dropDuplicates()
            .withColumn("year",       F.year("date_value"))
            .withColumn("month",      F.month("date_value"))
            .withColumn("quarter",    F.quarter("date_value"))
            .withColumn("day",        F.dayofmonth("date_value"))
            .withColumn("month_name", F.date_format("date_value", "MMMM"))
            .withColumn("day_name",   F.date_format("date_value", "EEEE"))
            .withColumn("is_weekend", F.dayofweek("date_value").isin([1, 7]))
        )

    def _build_fact_claims(self, df: DataFrame) -> DataFrame:
        """
        Build FACT_CLAIMS - one row per claim (500K rows).
        Central table of the star schema containing all measurable values.
        References dimension tables via surrogate keys.
        """
        return (
            df
            .withColumn("claim_key",
                        F.sha2(F.col("claim_id"), 256))
            .withColumn("member_key",
                        F.col("member_id"))
            .withColumn("drug_key",
                        F.sha2(F.col("ndc_code").cast("string"), 256))
            .withColumn("provider_key",
                        F.sha2(F.col("npi_number").cast("string"), 256))
            .select(
                "claim_key", "member_key", "drug_key", "provider_key",
                "service_date", "claim_amount", "quantity",
                "days_supply", "plan_id", "claim_status", "etl_processed_at",
            )
        )