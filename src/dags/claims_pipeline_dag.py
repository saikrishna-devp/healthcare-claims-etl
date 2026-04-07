"""
claims_pipeline_dag.py
Airflow DAG that orchestrates the full Healthcare Claims ETL pipeline.
Runs daily at 2 AM UTC with retry logic, SLA alerts, and failure notifications.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ------------------------------------------------------------------ #
#  Default args — applied to every task in this DAG
# ------------------------------------------------------------------ #
DEFAULT_ARGS = {
    "owner":            "saikrishna",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "email":            ["krishnasv207@gmail.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
    "sla":              timedelta(hours=4),     # Alert if DAG exceeds 4h
}


# ------------------------------------------------------------------ #
#  Task functions — each wraps one stage of the ETL
# ------------------------------------------------------------------ #
def extract_task(**context) -> None:
    """Pull claims data from S3 for yesterday's partition."""
    import yaml
    from src.etl.extract import ClaimsExtractor

    with open("/opt/airflow/config/config.yaml") as f:
        config = yaml.safe_load(f)

    extractor = ClaimsExtractor(config)
    df = extractor.extract_incremental(lookback_days=1)

    # Push row count to XCom for downstream logging
    context["ti"].xcom_push(key="raw_row_count", value=len(df))
    # Persist to temp parquet for next task
    df.to_parquet("/tmp/claims_raw.parquet", index=False)


def validate_task(**context) -> None:
    """Run Great Expectations data quality checks on raw data."""
    import pandas as pd
    from src.quality.validate import ClaimsValidator

    df = pd.read_parquet("/tmp/claims_raw.parquet")
    validator = ClaimsValidator()
    results = validator.run_checks(df)

    if not results["success"]:
        failed = [r for r in results["results"] if not r["success"]]
        raise ValueError(
            f"Data quality failed — {len(failed)} check(s) failed: "
            + str([r["expectation_config"]["expectation_type"] for r in failed])
        )

    context["ti"].xcom_push(key="dq_passed", value=True)


def transform_task(**context) -> None:
    """Apply PySpark transformations to raw claims data."""
    import pandas as pd
    from pyspark.sql import SparkSession
    from src.etl.transform import ClaimsTransformer, get_spark_session

    spark = get_spark_session()
    raw_pd = pd.read_parquet("/tmp/claims_raw.parquet")
    raw_df = spark.createDataFrame(raw_pd)

    transformer = ClaimsTransformer(spark)
    transformed = transformer.run(raw_df)

    # Persist each table as parquet for loader
    for table_name, table_df in transformed.items():
        table_df.write.mode("overwrite").parquet(f"/tmp/claims_{table_name}.parquet")

    context["ti"].xcom_push(
        key="fact_row_count",
        value=transformed["fact_claims"].count()
    )


def load_task(**context) -> None:
    """Load transformed DataFrames into Snowflake."""
    import yaml
    from pyspark.sql import SparkSession
    from src.etl.load import SnowflakeLoader

    with open("/opt/airflow/config/config.yaml") as f:
        config = yaml.safe_load(f)

    spark = SparkSession.builder.appName("ClaimsLoader").getOrCreate()

    transformed = {
        table: spark.read.parquet(f"/tmp/claims_{table}.parquet")
        for table in ["dim_drug", "dim_provider", "dim_date", "fact_claims"]
    }

    loader = SnowflakeLoader(config)
    loader.run(transformed)


def notify_success(**context) -> None:
    """Log pipeline success summary."""
    raw_count  = context["ti"].xcom_pull(key="raw_row_count",  task_ids="extract")
    fact_count = context["ti"].xcom_pull(key="fact_row_count", task_ids="transform")
    print(
        f"Pipeline SUCCESS — "
        f"raw: {raw_count:,} rows | "
        f"fact_claims loaded: {fact_count:,} rows | "
        f"run_date: {context['ds']}"
    )


def notify_failure(**context) -> None:
    """Alert on pipeline failure — extend with PagerDuty/Slack as needed."""
    print(f"PIPELINE FAILED on {context['ds']} — check task logs for details.")


# ------------------------------------------------------------------ #
#  DAG definition
# ------------------------------------------------------------------ #
with DAG(
    dag_id="healthcare_claims_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily ETL pipeline: S3 → PySpark → Great Expectations → Snowflake",
    schedule_interval="0 2 * * *",   # 2 AM UTC daily
    catchup=False,
    max_active_runs=1,
    tags=["healthcare", "claims", "etl", "snowflake"],
) as dag:

    start = EmptyOperator(task_id="start")

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_task,
        execution_timeout=timedelta(hours=1),
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=validate_task,
        execution_timeout=timedelta(minutes=30),
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
        execution_timeout=timedelta(hours=2),
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_task,
        execution_timeout=timedelta(hours=1),
    )

    success_notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    failure_notify = PythonOperator(
        task_id="notify_failure",
        python_callable=notify_failure,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Pipeline flow
    start >> extract >> validate >> transform >> load >> [success_notify, failure_notify] >> end
