"""
snowflake_conn.py
Builds Snowflake Spark connector options from config.
"""

import os


def get_snowflake_options(config: dict) -> dict:
    """
    Build the options dict required by the Snowflake Spark connector.
    Credentials are pulled from environment variables — never hardcoded.
    """
    sf = config["snowflake"]
    return {
        "sfURL":       f"{sf['account']}.snowflakecomputing.com",
        "sfUser":      os.environ["SNOWFLAKE_USER"],
        "sfPassword":  os.environ["SNOWFLAKE_PASSWORD"],
        "sfDatabase":  sf["database"],
        "sfSchema":    sf["schema"],
        "sfWarehouse": sf["warehouse"],
        "sfRole":      sf.get("role", "SYSADMIN"),
    }
