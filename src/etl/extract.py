"""
extract.py
Handles extraction of pharmacy claims data.
Local version: generates synthetic data or loads from CSV.
Production version: would pull from AWS S3 date partitions.
"""

import os
import random
import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)


class ClaimsExtractor:
    """
    Extracts pharmacy claims data.
    Locally: generates synthetic data or loads existing CSV.
    Production: pulls from AWS S3 partitioned by date.
    """

    def __init__(self, config: dict):
        self.raw_file  = config["pipeline"]["raw_file"]
        self.n_records = config["pipeline"]["n_records"]

    def extract(self) -> pd.DataFrame:
        """
        Main extract method.
        Loads existing file if present, otherwise generates fresh data.
        This makes the pipeline idempotent - safe to rerun.
        """
        if os.path.exists(self.raw_file):
            logger.info(f"Raw file found - loading from {self.raw_file}")
            df = pd.read_csv(self.raw_file)
            logger.info(f"Loaded {len(df):,} rows from existing file")
            return df

        logger.info(f"No raw file found - generating {self.n_records:,} synthetic records")
        return self._generate_synthetic_data()

    def _generate_synthetic_data(self) -> pd.DataFrame:
        """
        Generates realistic synthetic pharmacy claims data.
        Uses real NDC codes, drug names and realistic distributions.
        In production this data would come from pharmacy dispensing systems.
        """
        random.seed(42)

        drugs = [
            ("00002143301", "Insulin Glargine",  "DIABETES"),
            ("00069154041", "Apixaban",          "ANTICOAGULANT"),
            ("00310015130", "Atorvastatin",      "CHOLESTEROL"),
            ("00456345063", "Metformin",         "DIABETES"),
            ("00143988401", "Lisinopril",        "HYPERTENSION"),
            ("00074577490", "Adalimumab",        "IMMUNOLOGY"),
            ("00169368712", "Semaglutide",       "DIABETES"),
            ("00088221905", "Rivaroxaban",       "ANTICOAGULANT"),
            ("00378395193", "Amlodipine",        "HYPERTENSION"),
            ("00093221056", "Omeprazole",        "GASTROINTESTINAL"),
            ("00378718193", "Metoprolol",        "CARDIOLOGY"),
            ("00904584061", "Gabapentin",        "NEUROLOGY"),
            ("00071015523", "Rosuvastatin",      "CHOLESTEROL"),
            ("00003089121", "Apremilast",        "IMMUNOLOGY"),
            ("00054327099", "Oxycodone",         "PAIN"),
        ]

        states   = ["FL","TX","CA","NY","IL","PA","OH","GA","NC","MI",
                    "WA","AZ","MA","TN","IN","MO","MD","WI","CO","MN"]
        statuses = ["PAID"] * 80 + ["ADJUDICATED"] * 12 + ["REVERSED"] * 8
        plans    = [f"PLAN_{i:03d}" for i in range(1, 26)]

        rows = []
        for i in range(self.n_records):
            drug  = random.choice(drugs)
            month = random.randint(1, 12)
            day   = random.randint(1, 28)
            rows.append({
                "claim_id":     f"CLM{i + 1:08d}",
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
        os.makedirs(os.path.dirname(self.raw_file), exist_ok=True)
        df.to_csv(self.raw_file, index=False)
        logger.info(f"Generated {len(df):,} records - saved to {self.raw_file}")
        return df