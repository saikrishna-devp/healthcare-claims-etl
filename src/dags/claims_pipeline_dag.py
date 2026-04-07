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

DEFAULT_ARGS = {
    "owner":            "saikrishna",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "email":            ["krishnasv207@gmail.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
    "sla":              timedelta(hours=4),
}


def _load_config() -> dict:
    import yaml
    with open("/opt/airflow/config/config.yaml") as f:
        return yaml.safe_load(f)


def extract_task(**context) -> None:
    """Generate or load pharmacy claims data."""
    from src.etl.extract import ClaimsExtractor

    config    = _load_config()
    extractor = ClaimsExtractor(config)
    df        = extractor.extract()

    context["ti"].xcom_push(key="raw_row_count", value=len(df))
    df.to_parquet("/tmp/claims_raw.parquet", index=False)


def validate_task(**context) -> None:
    """Run 8 data quality checks on raw data."""
    import pandas as pd
    from src.quality.validate import ClaimsValidator

    df        = pd.read_parquet("/tmp/claims_raw.parquet")
    validator = ClaimsValidator()
    results   = validator.run_checks(df)

    if not results["success"]:
        failed = [r["check"] for r in results["results"] if not r["passed"]]
        raise ValueError(f"Data quality failed - {len(failed)} check(s) failed: {failed}")

    context["ti"].xcom_push(key="dq_passed", value=True)


def transform_task(**context) -> None:
    """Apply PySpark transformations and build star schema."""
    import pandas as pd
    from src.etl.transform import ClaimsTransformer, get_spark_session

    spark       = get_spark_session()
    raw_df      = pd.read_parquet("/tmp/claims_raw.parquet")
    transformer = ClaimsTransformer(spark)
    tables      = transformer.run(raw_df)

    for name, df in tables.items():
        df.to_parquet(f"/tmp/claims_{name}.parquet", index=False)

    context["ti"].xcom_push(key="fact_row_count", value=len(tables["fact_claims"]))
    spark.stop()


def load_task(**context) -> None:
    """Load transformed tables into PostgreSQL."""
    import pandas as pd
    from src.etl.load import ClaimsLoader

    config = _load_config()
    tables = {
        name: pd.read_parquet(f"/tmp/claims_{name}.parquet")
        for name in ["dim_drug", "dim_provider", "dim_date", "fact_claims"]
    }

    loader = ClaimsLoader(config)
    loader.run(tables)


def notify_success(**context) -> None:
    """Log pipeline success summary."""
    raw_count  = context["ti"].xcom_pull(key="raw_row_count",  task_ids="extract")
    fact_count = context["ti"].xcom_pull(key="fact_row_count", task_ids="transform")
    print(
        f"Pipeline SUCCESS | "
        f"raw: {raw_count:,} rows | "
        f"fact_claims: {fact_count:,} rows | "
        f"run_date: {context['ds']}"
    )


def notify_failure(**context) -> None:
    """Alert on pipeline failure."""
    print(f"PIPELINE FAILED on {context['ds']} - check task logs for details.")


with DAG(
    dag_id="healthcare_claims_pipeline",
    default_args=DEFAULT_ARGS,
    description="Daily ETL: Generate -> Validate -> PySpark Transform -> PostgreSQL",
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["healthcare", "claims", "etl", "pyspark", "postgresql"],
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

    start >> extract >> validate >> transform >> load >> [success_notify, failure_notify] >> end