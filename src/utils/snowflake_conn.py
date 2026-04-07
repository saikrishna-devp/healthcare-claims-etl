"""
snowflake_conn.py
Database connection helper.
Local version: connects to PostgreSQL.
Production version: connects to Snowflake.
The rest of the pipeline code does not change -
only this file needs updating to switch databases.
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from src.utils.logger import get_logger

logger = get_logger(__name__)


def get_postgres_engine(config: dict) -> Engine:
    """
    Build a SQLAlchemy engine for PostgreSQL.
    Used locally with Docker.

    Connection string format:
    dialect+driver://username:password@host:port/database
    """
    db = config["database"]
    conn_str = (
        f"postgresql+psycopg2://"
        f"{db['user']}:{db['password']}@"
        f"{db['host']}:{db['port']}/"
        f"{db['name']}"
    )
    logger.info(f"Connecting to PostgreSQL at {db['host']}:{db['port']}/{db['name']}")
    engine = create_engine(conn_str)
    logger.info("PostgreSQL engine created successfully")
    return engine


def get_snowflake_engine(config: dict) -> Engine:
    """
    Build a SQLAlchemy engine for Snowflake.
    Used in production with real cloud credentials.
    Credentials come from environment variables - never hardcoded.
    """
    sf = config["snowflake"]
    conn_str = (
        f"snowflake://"
        f"{os.environ['SNOWFLAKE_USER']}:{os.environ['SNOWFLAKE_PASSWORD']}@"
        f"{sf['account']}/"
        f"{sf['database']}/{sf['schema']}?"
        f"warehouse={sf['warehouse']}&role={sf['role']}"
    )
    logger.info(f"Connecting to Snowflake account: {sf['account']}")
    engine = create_engine(conn_str)
    logger.info("Snowflake engine created successfully")
    return engine