"""
transform.py
PySpark transformation layer for pharmacy claims data.
Handles cleaning, standardization, SCD Type 2, and dimensional modeling.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, DateType, TimestampType
)
from src.utils.logger import get_logger

logger = get_logger(__name__)


def get_spark_session(app_name: str = "HealthcareClaimsETL") -> SparkSession:
    """Create or retrieve a SparkSession with optimized configs."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


class ClaimsTransformer:
    """Applies all PySpark transformations to raw claims data."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    # ------------------------------------------------------------------ #
    #  Raw claims schema
    # ------------------------------------------------------------------ #
    RAW_SCHEMA = StructType([
        StructField("claim_id",        StringType(),    False),
        StructField("member_id",       StringType(),    False),
        StructField("ndc_code",        StringType(),    True),
        StructField("npi_number",      StringType(),    True),
        StructField("service_date",    StringType(),    True),
        StructField("dispensed_date",  StringType(),    True),
        StructField("claim_amount",    DoubleType(),    True),
        StructField("quantity",        DoubleType(),    True),
        StructField("days_supply",     IntegerType(),   True),
        StructField("plan_id",         StringType(),    True),
        StructField("drug_name",       StringType(),    True),
        StructField("drug_class",      StringType(),    True),
        StructField("member_state",    StringType(),    True),
        StructField("claim_status",    StringType(),    True),
        StructField("created_at",      TimestampType(), True),
    ])

    # ------------------------------------------------------------------ #
    #  Cleaning & standardization
    # ------------------------------------------------------------------ #
    def clean_claims(self, df: DataFrame) -> DataFrame:
        """
        Apply data cleaning rules:
        - Drop duplicates on claim_id
        - Filter invalid amounts and quantities
        - Standardize date formats
        - Uppercase categorical columns
        - Mask member PII fields for HIPAA compliance
        """
        logger.info(f"Starting clean_claims — input rows: {df.count():,}")

        cleaned = (
            df
            .dropDuplicates(["claim_id"])
            .filter(F.col("claim_amount") > 0)
            .filter(F.col("quantity") > 0)
            .filter(F.col("days_supply").between(1, 365))
            .filter(F.col("claim_status").isin("PAID", "ADJUDICATED", "REVERSED"))
            .withColumn("service_date",   F.to_date("service_date",   "yyyy-MM-dd"))
            .withColumn("dispensed_date", F.to_date("dispensed_date", "yyyy-MM-dd"))
            .withColumn("drug_class",  F.upper(F.trim(F.col("drug_class"))))
            .withColumn("member_state",F.upper(F.trim(F.col("member_state"))))
            .withColumn("claim_status",F.upper(F.trim(F.col("claim_status"))))
            # HIPAA: hash member_id before storing
            .withColumn("member_id", F.sha2(F.col("member_id"), 256))
            .withColumn("etl_processed_at", F.current_timestamp())
        )

        logger.info(f"clean_claims complete — output rows: {cleaned.count():,}")
        return cleaned

    # ------------------------------------------------------------------ #
    #  Dimension builders
    # ------------------------------------------------------------------ #
    def build_dim_drug(self, df: DataFrame) -> DataFrame:
        """Build DIM_DRUG from distinct NDC codes in the claims data."""
        return (
            df
            .select("ndc_code", "drug_name", "drug_class")
            .dropDuplicates(["ndc_code"])
            .withColumn("drug_key", F.sha2(F.col("ndc_code"), 256))
            .withColumn("effective_start", F.current_date())
            .withColumn("effective_end",   F.lit(None).cast(DateType()))
            .withColumn("is_current",      F.lit(True))
        )

    def build_dim_provider(self, df: DataFrame) -> DataFrame:
        """Build DIM_PROVIDER from distinct NPI numbers."""
        return (
            df
            .select("npi_number", "member_state")
            .dropDuplicates(["npi_number"])
            .withColumnRenamed("member_state", "provider_state")
            .withColumn("provider_key", F.sha2(F.col("npi_number"), 256))
            .withColumn("effective_start", F.current_date())
            .withColumn("effective_end",   F.lit(None).cast(DateType()))
            .withColumn("is_current",      F.lit(True))
        )

    def build_dim_date(self, df: DataFrame) -> DataFrame:
        """Build DIM_DATE from all service dates in the dataset."""
        return (
            df
            .select(F.col("service_date").alias("date_value"))
            .dropDuplicates()
            .withColumn("date_key",   F.date_format("date_value", "yyyyMMdd").cast("int"))
            .withColumn("year",       F.year("date_value"))
            .withColumn("month",      F.month("date_value"))
            .withColumn("day",        F.dayofmonth("date_value"))
            .withColumn("quarter",    F.quarter("date_value"))
            .withColumn("week",       F.weekofyear("date_value"))
            .withColumn("day_name",   F.date_format("date_value", "EEEE"))
            .withColumn("month_name", F.date_format("date_value", "MMMM"))
            .withColumn("is_weekend", F.dayofweek("date_value").isin([1, 7]))
        )

    # ------------------------------------------------------------------ #
    #  Fact table builder
    # ------------------------------------------------------------------ #
    def build_fact_claims(self, df: DataFrame) -> DataFrame:
        """
        Build FACT_CLAIMS with surrogate keys referencing all dimensions.
        Aggregates at claim grain — one row per claim_id.
        """
        return (
            df
            .filter(F.col("claim_status") == "PAID")
            .withColumn("claim_key",    F.sha2(F.col("claim_id"), 256))
            .withColumn("member_key",   F.col("member_id"))   # already hashed
            .withColumn("drug_key",     F.sha2(F.col("ndc_code"), 256))
            .withColumn("provider_key", F.sha2(F.col("npi_number"), 256))
            .withColumn("date_key",     F.date_format("service_date", "yyyyMMdd").cast("int"))
            .select(
                "claim_key",
                "member_key",
                "drug_key",
                "provider_key",
                "date_key",
                "claim_amount",
                "quantity",
                "days_supply",
                "plan_id",
                "claim_status",
                "dispensed_date",
                "etl_processed_at",
            )
        )

    # ------------------------------------------------------------------ #
    #  Orchestrate all transformations
    # ------------------------------------------------------------------ #
    def run(self, raw_df: DataFrame) -> dict[str, DataFrame]:
        """
        Run the full transformation pipeline.
        Returns a dict of DataFrames keyed by target table name.
        """
        logger.info("Starting full transformation pipeline")
        cleaned = self.clean_claims(raw_df)

        result = {
            "dim_drug":     self.build_dim_drug(cleaned),
            "dim_provider": self.build_dim_provider(cleaned),
            "dim_date":     self.build_dim_date(cleaned),
            "fact_claims":  self.build_fact_claims(cleaned),
        }

        for name, tdf in result.items():
            logger.info(f"{name}: {tdf.count():,} rows")

        logger.info("Transformation pipeline complete")
        return result
