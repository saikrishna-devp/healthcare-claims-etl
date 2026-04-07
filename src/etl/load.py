"""
load.py
Loads transformed DataFrames into PostgreSQL (primary)
and SQLite (backup).
In production this would load into Snowflake.
"""

import sqlite3
import pandas as pd
from sqlalchemy import create_engine
from src.utils.logger import get_logger

logger = get_logger(__name__)


class ClaimsLoader:
    """
    Loads transformed DataFrames into the data warehouse.
    Primary: PostgreSQL (running in Docker)
    Backup:  SQLite (local file - always works)
    """

    def __init__(self, config: dict):
        self.db_host = config["database"]["host"]
        self.db_port = config["database"]["port"]
        self.db_name = config["database"]["name"]
        self.db_user = config["database"]["user"]
        self.db_pass = config["database"]["password"]
        self.db_file = config["pipeline"]["db_file"]

    def run(self, tables: dict) -> None:
        """
        Load all tables to PostgreSQL and SQLite.
        Dimensions load first, fact table last.
        """
        logger.info("Starting load sequence")
        logger.info("Order: dim_drug -> dim_provider -> dim_date -> fact_claims")

        self._load_postgres(tables)
        self._load_sqlite(tables)

        logger.info("All tables loaded successfully")

    def _load_postgres(self, tables: dict) -> None:
        """
        Load all tables into PostgreSQL.
        if_exists='replace' makes the pipeline idempotent - safe to rerun.
        """
        try:
            conn_str = (
                f"postgresql+psycopg2://"
                f"{self.db_user}:{self.db_pass}@"
                f"{self.db_host}:{self.db_port}/"
                f"{self.db_name}"
            )
            engine = create_engine(conn_str)

            for name, df in tables.items():
                df.to_sql(name, engine, if_exists="replace", index=False)
                logger.info(f"  [PostgreSQL] {len(df):,} rows -> {name}")

            logger.info("PostgreSQL load complete")

        except Exception as e:
            logger.error(f"PostgreSQL load failed: {e}")
            logger.info("Falling back to SQLite...")

    def _load_sqlite(self, tables: dict) -> None:
        """
        Load all tables into SQLite as backup.
        SQLite is file-based - no server needed.
        """
        conn = sqlite3.connect(self.db_file)

        for name, df in tables.items():
            df.to_sql(name, conn, if_exists="replace", index=False)
            logger.info(f"  [SQLite] {len(df):,} rows -> {name}")

        conn.close()
        logger.info(f"SQLite backup saved to {self.db_file}")