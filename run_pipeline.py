"""
run_pipeline.py
Healthcare Claims ETL Pipeline
PySpark + PostgreSQL (local) version
Run with Python 3.11
"""

import os
import time
import random
import sqlite3
import pandas as pd
from datetime import datetime
from hashlib import sha256

# Tell PySpark to use Python 3.11
os.environ["PYSPARK_PYTHON"]        = r"C:\Users\saira\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\saira\AppData\Local\Programs\Python\Python311\python.exe"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ─────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────
RAW_FILE = "data/raw_claims.csv"
DB_FILE  = "data/healthcare_dwh.db"
LOG_FILE = "data/pipeline.log"
os.makedirs("data", exist_ok=True)

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "healthcare_dwh"
DB_USER = "admin"
DB_PASS = "admin123"

# ─────────────────────────────────────────
#  LOGGER
# ─────────────────────────────────────────
def log(msg):
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} | {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")

# ─────────────────────────────────────────
#  SPARK SESSION
# ─────────────────────────────────────────
def get_spark():
    return (
        SparkSession.builder
        .appName("HealthcareClaimsETL")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .getOrCreate()
    )

# ─────────────────────────────────────────
#  STEP 1 - EXTRACT
# ─────────────────────────────────────────
def extract() -> pd.DataFrame:
    log("=" * 60)
    log("STEP 1 - EXTRACT")
    log("=" * 60)

    if os.path.exists(RAW_FILE):
        df = pd.read_csv(RAW_FILE)
        log(f"Loaded {len(df):,} rows from existing file")
        return df

    log("Generating 500,000 realistic pharmacy claims records...")
    return generate_data()


def generate_data() -> pd.DataFrame:
    random.seed(42)

    drugs = [
        ("00002143301", "Insulin Glargine",   "DIABETES"),
        ("00069154041", "Apixaban",           "ANTICOAGULANT"),
        ("00310015130", "Atorvastatin",       "CHOLESTEROL"),
        ("00456345063", "Metformin",          "DIABETES"),
        ("00143988401", "Lisinopril",         "HYPERTENSION"),
        ("00074577490", "Adalimumab",         "IMMUNOLOGY"),
        ("00169368712", "Semaglutide",        "DIABETES"),
        ("00088221905", "Rivaroxaban",        "ANTICOAGULANT"),
        ("00378395193", "Amlodipine",         "HYPERTENSION"),
        ("00093221056", "Omeprazole",         "GASTROINTESTINAL"),
        ("00378718193", "Metoprolol",         "CARDIOLOGY"),
        ("00904584061", "Gabapentin",         "NEUROLOGY"),
        ("00071015523", "Rosuvastatin",       "CHOLESTEROL"),
        ("00003089121", "Apremilast",         "IMMUNOLOGY"),
        ("00054327099", "Oxycodone",          "PAIN"),
    ]
    states   = ["FL","TX","CA","NY","IL","PA","OH","GA","NC","MI",
                "WA","AZ","MA","TN","IN","MO","MD","WI","CO","MN"]
    statuses = ["PAID"]*80 + ["ADJUDICATED"]*12 + ["REVERSED"]*8
    plans    = [f"PLAN_{i:03d}" for i in range(1, 26)]

    rows = []
    for i in range(500000):
        drug  = random.choice(drugs)
        month = random.randint(1, 12)
        day   = random.randint(1, 28)
        rows.append({
            "claim_id":     f"CLM{i+1:08d}",
            "member_id":    f"MBR{random.randint(1, 15000):06d}",
            "ndc_code":     drug[0],
            "drug_name":    drug[1],
            "drug_class":   drug[2],
            "npi_number":   str(random.randint(1000000000, 9999999999)),
            "service_date": f"2024-{month:02d}-{day:02d}",
            "claim_amount": round(random.uniform(10, 8000), 2),
            "quantity":     float(random.choice([30, 60, 90, 180])),
            "days_supply":  random.choice([30, 60, 90]),
            "plan_id":      random.choice(plans),
            "member_state": random.choice(states),
            "claim_status": random.choice(statuses),
        })

    df = pd.DataFrame(rows)
    df.to_csv(RAW_FILE, index=False)
    log(f"Generated {len(df):,} records -> saved to {RAW_FILE}")
    return df


# ─────────────────────────────────────────
#  STEP 2 - VALIDATE
# ─────────────────────────────────────────
def validate(df: pd.DataFrame) -> pd.DataFrame:
    log("=" * 60)
    log("STEP 2 - VALIDATE (Data Quality Checks)")
    log("=" * 60)

    checks = []

    def check(name, passed, detail=""):
        status = "PASS" if passed else "FAIL"
        log(f"  [{status}] {name} {detail}")
        checks.append(passed)

    check("Row count in valid range",
          1000 <= len(df) <= 10_000_000,
          f"({len(df):,} rows)")

    check("No null claim_ids",
          df["claim_id"].notna().all(),
          f"({df['claim_id'].isna().sum()} nulls)")

    check("No duplicate claim_ids",
          df["claim_id"].nunique() == len(df),
          f"({df['claim_id'].nunique():,} unique)")

    check("claim_amount is positive",
          (df["claim_amount"] > 0).all(),
          f"(min={df['claim_amount'].min():.2f}, max={df['claim_amount'].max():.2f})")

    check("quantity is positive",
          (df["quantity"] > 0).all(),
          f"(min={df['quantity'].min()})")

    check("days_supply between 1-365",
          df["days_supply"].between(1, 365).all(),
          f"(values: {sorted(df['days_supply'].unique().tolist())})")

    valid_statuses = {"PAID", "ADJUDICATED", "REVERSED", "PENDING"}
    check("claim_status values valid",
          df["claim_status"].isin(valid_statuses).all(),
          f"(found: {df['claim_status'].unique().tolist()})")

    check("service_date is parseable",
          pd.to_datetime(df["service_date"], errors="coerce").notna().all(),
          "")

    passed = sum(checks)
    total  = len(checks)
    log(f"\n  Result: {passed}/{total} checks passed")
    return df


# ─────────────────────────────────────────
#  STEP 3 - TRANSFORM (PySpark)
# ─────────────────────────────────────────
def transform(df: pd.DataFrame) -> dict:
    log("=" * 60)
    log("STEP 3 - TRANSFORM (PySpark)")
    log("=" * 60)

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    log(f"Creating Spark DataFrame from {len(df):,} rows...")
    sdf = spark.createDataFrame(df)

    log("Applying cleaning transformations...")
    cleaned = (
        sdf
        .dropDuplicates(["claim_id"])
        .withColumn("claim_amount", F.col("claim_amount").cast("double"))
        .withColumn("quantity",     F.col("quantity").cast("double"))
        .withColumn("days_supply",  F.col("days_supply").cast("integer"))
        .withColumn("service_date", F.to_date(F.col("service_date"), "yyyy-MM-dd"))
        .withColumn("drug_class",   F.upper(F.trim(F.col("drug_class"))))
        .withColumn("claim_status", F.upper(F.trim(F.col("claim_status"))))
        .withColumn("member_id",    F.sha2(F.col("member_id"), 256))
        .withColumn("etl_processed_at", F.current_timestamp())
        .filter(F.col("claim_amount") > 0)
        .filter(F.col("quantity") > 0)
        .filter(F.col("days_supply").between(1, 365))
    )

    log("Building dimension tables...")

    dim_drug = (
        cleaned
        .select("ndc_code", "drug_name", "drug_class")
        .dropDuplicates(["ndc_code"])
        .withColumn("drug_key", F.sha2(F.col("ndc_code").cast("string"), 256))
    )

    dim_provider = (
        cleaned
        .select("npi_number", "member_state")
        .dropDuplicates(["npi_number"])
        .withColumn("provider_key", F.sha2(F.col("npi_number").cast("string"), 256))
    )

    dim_date = (
        cleaned
        .select(F.col("service_date").alias("date_value"))
        .dropDuplicates()
        .withColumn("year",      F.year("date_value"))
        .withColumn("month",     F.month("date_value"))
        .withColumn("quarter",   F.quarter("date_value"))
        .withColumn("day",       F.dayofmonth("date_value"))
        .withColumn("month_name",F.date_format("date_value", "MMMM"))
        .withColumn("day_name",  F.date_format("date_value", "EEEE"))
    )

    fact_claims = (
        cleaned
        .withColumn("claim_key",    F.sha2(F.col("claim_id"), 256))
        .withColumn("member_key",   F.col("member_id"))
        .withColumn("drug_key",     F.sha2(F.col("ndc_code").cast("string"), 256))
        .withColumn("provider_key", F.sha2(F.col("npi_number").cast("string"), 256))
        .select(
            "claim_key", "member_key", "drug_key", "provider_key",
            "service_date", "claim_amount", "quantity",
            "days_supply", "plan_id", "claim_status", "etl_processed_at"
        )
    )

    log("Converting Spark DataFrames to Pandas for loading...")
    result = {
        "dim_drug":     dim_drug.toPandas(),
        "dim_provider": dim_provider.toPandas(),
        "dim_date":     dim_date.toPandas(),
        "fact_claims":  fact_claims.toPandas(),
    }

    for name, tdf in result.items():
        log(f"  {name}: {len(tdf):,} rows")

    spark.stop()
    return result


# ─────────────────────────────────────────
#  STEP 4 - LOAD (PostgreSQL + SQLite)
# ─────────────────────────────────────────
def load(tables: dict):
    log("=" * 60)
    log("STEP 4 - LOAD (PostgreSQL + SQLite backup)")
    log("=" * 60)

    # Load to PostgreSQL
    try:
        from sqlalchemy import create_engine
        engine = create_engine(
            f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )
        for name, df in tables.items():
            df.to_sql(name, engine, if_exists="replace", index=False)
            log(f"  [PostgreSQL] Loaded {len(df):,} rows -> {name}")
        log("  PostgreSQL load complete!")
    except Exception as e:
        log(f"  PostgreSQL load failed: {e}")
        log("  Falling back to SQLite...")

    # Always save to SQLite as backup
    conn = sqlite3.connect(DB_FILE)
    for name, df in tables.items():
        df.to_sql(name, conn, if_exists="replace", index=False)
    conn.close()
    log(f"  [SQLite] All tables saved to {DB_FILE}")


# ─────────────────────────────────────────
#  STEP 5 - ANALYTICS
# ─────────────────────────────────────────
def show_results():
    log("=" * 60)
    log("STEP 5 - ANALYTICS RESULTS")
    log("=" * 60)

    conn = sqlite3.connect(DB_FILE)

    queries = {
        "Pipeline summary": """
            SELECT
                COUNT(*)                    AS total_claims,
                COUNT(DISTINCT member_key)  AS unique_members,
                COUNT(DISTINCT drug_key)    AS unique_drugs,
                ROUND(SUM(claim_amount),2)  AS total_amount,
                ROUND(AVG(claim_amount),2)  AS avg_claim,
                ROUND(MIN(claim_amount),2)  AS min_claim,
                ROUND(MAX(claim_amount),2)  AS max_claim
            FROM fact_claims
        """,
        "Claims by status": """
            SELECT claim_status,
                   COUNT(*) AS claims,
                   ROUND(SUM(claim_amount),2) AS total_amount,
                   ROUND(AVG(claim_amount),2) AS avg_amount
            FROM fact_claims
            GROUP BY claim_status
            ORDER BY claims DESC
        """,
        "Top 5 drug classes by revenue": """
            SELECT d.drug_class,
                   COUNT(*) AS claims,
                   ROUND(SUM(f.claim_amount),2) AS total_revenue,
                   ROUND(AVG(f.claim_amount),2) AS avg_cost
            FROM fact_claims f
            JOIN dim_drug d ON f.drug_key = d.drug_key
            GROUP BY d.drug_class
            ORDER BY total_revenue DESC
            LIMIT 5
        """,
        "Top 5 most prescribed drugs": """
            SELECT d.drug_name, d.drug_class,
                   COUNT(*) AS prescriptions,
                   ROUND(AVG(f.claim_amount),2) AS avg_cost
            FROM fact_claims f
            JOIN dim_drug d ON f.drug_key = d.drug_key
            GROUP BY d.drug_name, d.drug_class
            ORDER BY prescriptions DESC
            LIMIT 5
        """,
        "Top 5 states by volume": """
            SELECT p.member_state AS state,
                   COUNT(*) AS claims,
                   ROUND(SUM(f.claim_amount),2) AS total_amount
            FROM fact_claims f
            JOIN dim_provider p ON f.provider_key = p.provider_key
            GROUP BY p.member_state
            ORDER BY claims DESC
            LIMIT 5
        """,
        "Claims by quarter": """
            SELECT dt.year, dt.quarter,
                   COUNT(*) AS claims,
                   ROUND(SUM(f.claim_amount),2) AS total_amount
            FROM fact_claims f
            JOIN dim_date dt ON date(f.service_date) = date(dt.date_value)
            GROUP BY dt.year, dt.quarter
            ORDER BY dt.year, dt.quarter
        """,
    }

    for title, sql in queries.items():
        print(f"\n  {'─'*50}")
        print(f"  {title}")
        print(f"  {'─'*50}")
        result = pd.read_sql_query(sql, conn)
        print(result.to_string(index=False))

    conn.close()


# ─────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────
if __name__ == "__main__":
    start = time.time()

    log("=" * 60)
    log("  HEALTHCARE CLAIMS ETL PIPELINE STARTING")
    log(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 60)

    raw_df   = extract()
    clean_df = validate(raw_df)
    tables   = transform(clean_df)
    load(tables)
    show_results()

    elapsed = round(time.time() - start, 1)
    log("\n" + "=" * 60)
    log(f"  PIPELINE COMPLETE in {elapsed} seconds!")
    log(f"  {len(tables['fact_claims']):,} claims loaded to warehouse")
    log(f"  Database : {DB_FILE}")
    log(f"  Log file : {LOG_FILE}")
    log("=" * 60)