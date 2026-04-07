"""
load.py
Loads transformed DataFrames into Snowflake using the Snowflake Spark connector.
Supports MERGE (upsert) for dimensions and INSERT for fact tables.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.utils.snowflake_conn import get_snowflake_options
from src.utils.logger import get_logger

logger = get_logger(__name__)


class SnowflakeLoader:
    """Loads PySpark DataFrames into Snowflake tables."""

    def __init__(self, config: dict):
        self.sf_options = get_snowflake_options(config)
        self.database  = config["snowflake"]["database"]
        self.schema    = config["snowflake"]["schema"]

    def _table_uri(self, table_name: str) -> str:
        return f"{self.database}.{self.schema}.{table_name.upper()}"

    # ------------------------------------------------------------------ #
    #  Write helpers
    # ------------------------------------------------------------------ #
    def _write(self, df: DataFrame, table: str, mode: str = "append") -> None:
        """Generic write to Snowflake."""
        logger.info(f"Writing {df.count():,} rows to {self._table_uri(table)} (mode={mode})")
        (
            df.write
            .format("net.snowflake.spark.snowflake")
            .options(**self.sf_options)
            .option("dbtable", self._table_uri(table))
            .mode(mode)
            .save()
        )
        logger.info(f"Write to {table} complete")

    def load_fact_claims(self, df: DataFrame) -> None:
        """
        Append-only load for FACT_CLAIMS.
        Idempotent — duplicate claim_keys are rejected by the Snowflake PK constraint.
        """
        self._write(df, "fact_claims", mode="append")

    def load_dimension(self, df: DataFrame, table: str, key_col: str) -> None:
        """
        SCD Type 1 upsert for dimension tables.
        New keys are inserted; existing keys update non-key columns.
        """
        logger.info(f"Upserting {self._table_uri(table)} on key: {key_col}")

        # Stage to a temp table, then MERGE in Snowflake
        stage_table = f"{table}_stage"
        self._write(df, stage_table, mode="overwrite")

        merge_sql = f"""
            MERGE INTO {self._table_uri(table)} AS target
            USING {self._table_uri(stage_table)} AS source
            ON target.{key_col} = source.{key_col}
            WHEN MATCHED THEN UPDATE SET
                target.effective_start = source.effective_start,
                target.is_current      = source.is_current
            WHEN NOT MATCHED THEN INSERT (
                SELECT * FROM source
            );
        """
        self._execute_sql(merge_sql)
        logger.info(f"Upsert to {table} complete")

    def _execute_sql(self, sql: str) -> None:
        """Execute raw SQL in Snowflake (used for MERGE statements)."""
        import snowflake.connector
        conn = snowflake.connector.connect(
            user=self.sf_options["sfUser"],
            password=self.sf_options["sfPassword"],
            account=self.sf_options["sfURL"].replace("https://", "").replace(".snowflakecomputing.com", ""),
            warehouse=self.sf_options["sfWarehouse"],
            database=self.database,
            schema=self.schema,
        )
        try:
            cur = conn.cursor()
            cur.execute(sql)
            logger.info(f"SQL executed: {sql[:80]}...")
        finally:
            conn.close()

    # ------------------------------------------------------------------ #
    #  Orchestrate all loads
    # ------------------------------------------------------------------ #
    def run(self, transformed: dict) -> None:
        """
        Load all transformed DataFrames into Snowflake.
        Order matters: dimensions before facts.
        """
        logger.info("Starting Snowflake load sequence")

        self.load_dimension(transformed["dim_drug"],     "dim_drug",     "drug_key")
        self.load_dimension(transformed["dim_provider"], "dim_provider", "provider_key")
        self.load_dimension(transformed["dim_date"],     "dim_date",     "date_key")
        self.load_fact_claims(transformed["fact_claims"])

        logger.info("All tables loaded to Snowflake successfully")
