# Healthcare Claims ETL Pipeline

An end-to-end data pipeline that processes 500K pharmacy claims daily using PySpark for transformation, PostgreSQL as the data warehouse, and Apache Airflow for orchestration — with automated data quality validation at every stage.

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-4.1-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Airflow](https://img.shields.io/badge/Airflow-2.7-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=flat-square&logo=postgresql&logoColor=white)](https://postgresql.org)
[![Docker](https://img.shields.io/badge/Docker-29.3-2496ED?style=flat-square&logo=docker&logoColor=white)](https://docker.com)

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Data Source                          │
│         Synthetic Pharmacy Claims Generator             │
│         (mirrors real CMS Medicare claims format)       │
└─────────────────────────┬───────────────────────────────┘
                          │  500,000 records
                          ▼
┌─────────────────────────────────────────────────────────┐
│              STEP 2 - Validate                          │
│         8 Data Quality Checks                           │
│   null checks, dedup, ranges, formats, valid values     │
└─────────────────────────┬───────────────────────────────┘
                          │  All checks PASS
                          ▼
┌─────────────────────────────────────────────────────────┐
│              STEP 3 - Transform (PySpark)               │
│                                                         │
│  Clean -> Hash PII -> Build Star Schema                 │
│                                                         │
│  dim_drug (15 rows)     fact_claims (500,000 rows)      │
│  dim_provider           dim_date (336 rows)             │
└─────────────────────────┬───────────────────────────────┘
                          │  4 tables
                          ▼
┌─────────────────────────────────────────────────────────┐
│              STEP 4 - Load                              │
│   Primary: PostgreSQL (Docker)                          │
│   Backup:  SQLite (local file)                          │
└─────────────────────────┬───────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│              STEP 5 - Analytics                         │
│   Revenue by drug class, top drugs, claims by state     │
└─────────────────────────────────────────────────────────┘
```

---

## Star Schema

```
                    ┌──────────────────┐
                    │   FACT_CLAIMS    │
                    │──────────────────│
                    │ claim_key  (PK)  │
                    │ member_key (FK)  │  SHA256 hashed (HIPAA)
                    │ drug_key   (FK)  │
                    │ provider_key(FK) │
                    │ service_date     │
                    │ claim_amount     │
                    │ quantity         │
                    │ days_supply      │
                    │ plan_id          │
                    │ claim_status     │
                    │ etl_processed_at │
                    └────────┬─────────┘
         ┌──────────────────┼──────────────────┐
         │                  │                  │
┌────────▼───────┐  ┌───────▼──────┐  ┌────────▼──────┐
│   DIM_DRUG     │  │   DIM_DATE   │  │ DIM_PROVIDER  │
│────────────────│  │──────────────│  │───────────────│
│ drug_key  (PK) │  │ date_value   │  │ provider_key  │
│ ndc_code       │  │ year         │  │ npi_number    │
│ drug_name      │  │ month        │  │ member_state  │
│ drug_class     │  │ quarter      │  │               │
│                │  │ month_name   │  │               │
│ 15 rows        │  │ 336 rows     │  │ 499,999 rows  │
└────────────────┘  └──────────────┘  └───────────────┘
```

---

## Project Structure

```
healthcare-claims-etl/
├── src/
│   ├── etl/
│   │   ├── extract.py          # Data generation and loading
│   │   ├── transform.py        # PySpark transformations + star schema
│   │   └── load.py             # PostgreSQL + SQLite loader
│   ├── dags/
│   │   └── claims_pipeline_dag.py   # Airflow DAG definition
│   ├── quality/
│   │   └── validate.py         # 8 data quality checks
│   └── utils/
│       ├── logger.py           # Centralized logging
│       └── snowflake_conn.py   # DB connection helper
├── config/
│   └── config.yaml             # Pipeline configuration
├── docker-compose.yml          # PostgreSQL via Docker
├── run_pipeline.py             # Single script to run everything
├── requirements.txt
└── README.md
```

---

## ⚙️ Setup & Installation

### Prerequisites
- Python 3.11
- Java JDK 11+ (required for PySpark)
- Docker Desktop

### 1. Clone the repository
```bash
git clone https://github.com/saikrishna-devp/healthcare-claims-etl.git
cd healthcare-claims-etl
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Start PostgreSQL via Docker
```bash
docker run --name claims-db \
  -e POSTGRES_USER=admin \
  -e POSTGRES_PASSWORD=admin123 \
  -e POSTGRES_DB=healthcare_dwh \
  -p 5432:5432 -d postgres:15
```

### 4. Set PySpark Python path (Windows)
```bash
$env:PYSPARK_PYTHON="C:\Path\To\Python311\python.exe"
$env:PYSPARK_DRIVER_PYTHON="C:\Path\To\Python311\python.exe"
```

### 5. Run the pipeline
```bash
& "C:\Path\To\Python311\python.exe" run_pipeline.py
```

---

## Pipeline Results

| Metric | Value |
|---|---|
| Records processed | 500,000 |
| Pipeline runtime | ~161 seconds |
| Data quality checks | 8/8 passed |
| Tables loaded | 4 (1 fact + 3 dimensions) |
| Total revenue processed | $2 billion+ |
| Unique drugs | 15 |
| Unique members | 14,509 |
| Unique dates | 336 |

### Sample Analytics Output
```
Drug Class Revenue:
  DIABETES       ->  $79,447,770
  CHOLESTEROL    ->  $53,475,783
  ANTICOAGULANT  ->  $53,441,789
  HYPERTENSION   ->  $53,019,843
  IMMUNOLOGY     ->  $52,819,563

Top Prescribed Drugs:
  Insulin Glargine  ->  3,421 prescriptions
  Metoprolol        ->  3,410 prescriptions
  Atorvastatin      ->  3,403 prescriptions
```

---

## Key Features

- **PySpark processing** - distributed transformation across all CPU cores
- **Star schema design** - fact + 3 dimension tables optimized for analytics
- **8 automated quality checks** - nulls, duplicates, ranges, valid values
- **HIPAA compliant** - member IDs hashed with SHA256, no PII stored
- **Idempotent pipeline** - safe to rerun, same result every time
- **Dual storage** - PostgreSQL (primary) + SQLite (backup)
- **Airflow DAG** - scheduled at 2 AM daily with retry logic and SLA alerts
- **Centralized logging** - every step logged with timestamps for debugging

---

## 🔄 How It Works

**Extract:** Generates 500K realistic pharmacy claims with real NDC drug codes, NPI provider numbers, and authentic claim distributions (80% PAID, 12% ADJUDICATED, 8% REVERSED).

**Validate:** Runs 8 quality checks before any transformation. Catches nulls, duplicates, invalid amounts, bad dates, and unknown status values. Pipeline stops if critical checks fail.

**Transform:** PySpark cleans the data, hashes member IDs for HIPAA compliance, and builds a star schema separating measurable facts from descriptive dimensions.

**Load:** Writes all 4 tables to PostgreSQL via SQLAlchemy. SQLite backup ensures analytics always work even without the database server.

**Analytics:** Runs SQL queries joining fact and dimension tables to produce business insights - top drugs by revenue, claims by state, quarterly trends.

---
---

## Two Ways to Run

### Option 1 — Local Demo (run_pipeline.py)
Single script that runs the complete pipeline locally.
No cloud accounts needed — just Python, Docker, and Java.
```bash
# Start PostgreSQL
docker run --name claims-db \
  -e POSTGRES_USER=admin \
  -e POSTGRES_PASSWORD=admin123 \
  -e POSTGRES_DB=healthcare_dwh \
  -p 5432:5432 -d postgres:15

# Run the pipeline
& "C:\Path\To\Python311\python.exe" run_pipeline.py
```

What it does:
- Generates 500K realistic pharmacy claims
- Runs 8 data quality checks
- Transforms with PySpark into star schema
- Loads into PostgreSQL + SQLite
- Prints analytics results

### Option 2 — Production Architecture (src/ folder)
The `src/` folder contains the modular production-ready version designed for:
- **Apache Airflow** orchestration — scheduled daily at 2 AM
- **Scalable deployment** on cloud infrastructure
- **Team collaboration** — each module maintained independently
- **Easy extension** — swap PostgreSQL for Snowflake by changing one file
```bash
# Start Airflow locally
docker-compose up -d

# Access Airflow UI
# http://localhost:8080
# Enable DAG: healthcare_claims_pipeline
```

> The local demo and production architecture use identical business logic.
> The only difference is infrastructure — one runs on your laptop,
> the other runs on enterprise cloud infrastructure.
## Connect

**Saikrishna Suryavamsham** - Senior Data Engineer - Tampa, FL

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=flat-square&logo=linkedin&logoColor=white)](https://linkedin.com/in/sai207)
[![Email](https://img.shields.io/badge/Email-EA4335?style=flat-square&logo=gmail&logoColor=white)](mailto:krishnasv207@gmail.com)