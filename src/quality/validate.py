"""
validate.py
Data quality validation for pharmacy claims data.
Runs 8 checks on raw data before transformation.
If any critical check fails, pipeline stops.
"""

import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)


class ClaimsValidator:
    """
    Runs data quality checks on raw claims data.
    Think of this as a quality inspector at a factory -
    checks every batch before it moves to the next stage.
    """

    def __init__(self):
        self.results = []

    def run_checks(self, df: pd.DataFrame) -> dict:
        """
        Execute all 8 data quality checks.
        Returns summary dict with pass/fail counts.
        """
        self.results = []
        logger.info(f"Running data quality checks on {len(df):,} rows")

        self._check_row_count(df)
        self._check_no_null_claim_ids(df)
        self._check_no_duplicate_claim_ids(df)
        self._check_claim_amount_positive(df)
        self._check_quantity_positive(df)
        self._check_days_supply_range(df)
        self._check_valid_claim_status(df)
        self._check_service_date_parseable(df)

        passed    = sum(r["passed"] for r in self.results)
        total     = len(self.results)
        all_passed = passed == total

        logger.info(f"Result: {passed}/{total} checks passed")

        return {
            "success": all_passed,
            "passed":  passed,
            "total":   total,
            "results": self.results,
        }

    def _check(self, name: str, passed: bool, detail: str = "") -> None:
        """Helper to log and store each check result."""
        status = "PASS" if passed else "FAIL"
        logger.info(f"  [{status}] {name} {detail}")
        self.results.append({
            "check":  name,
            "passed": passed,
            "detail": detail,
        })

    def _check_row_count(self, df: pd.DataFrame) -> None:
        """Too few rows = upstream issue. Too many = possible duplication."""
        count = len(df)
        self._check(
            "Row count in valid range",
            1_000 <= count <= 10_000_000,
            f"({count:,} rows)"
        )

    def _check_no_null_claim_ids(self, df: pd.DataFrame) -> None:
        """Every claim must have an ID. Null = unidentifiable record."""
        null_count = df["claim_id"].isna().sum()
        self._check(
            "No null claim_ids",
            null_count == 0,
            f"({null_count} nulls found)"
        )

    def _check_no_duplicate_claim_ids(self, df: pd.DataFrame) -> None:
        """Each claim_id must be unique. Duplicates double-count costs."""
        unique = df["claim_id"].nunique()
        total  = len(df)
        self._check(
            "No duplicate claim_ids",
            unique == total,
            f"({unique:,} unique out of {total:,})"
        )

    def _check_claim_amount_positive(self, df: pd.DataFrame) -> None:
        """Zero or negative amounts indicate data errors."""
        invalid = (df["claim_amount"] <= 0).sum()
        self._check(
            "claim_amount is positive",
            invalid == 0,
            f"(min={df['claim_amount'].min():.2f}, max={df['claim_amount'].max():.2f})"
        )

    def _check_quantity_positive(self, df: pd.DataFrame) -> None:
        """Dispensed quantity must be positive."""
        invalid = (df["quantity"] <= 0).sum()
        self._check(
            "quantity is positive",
            invalid == 0,
            f"(min={df['quantity'].min()})"
        )

    def _check_days_supply_range(self, df: pd.DataFrame) -> None:
        """Days supply must be between 1 and 365. Common: 30, 60, 90."""
        invalid = (~df["days_supply"].between(1, 365)).sum()
        self._check(
            "days_supply between 1-365",
            invalid == 0,
            f"(values: {sorted(df['days_supply'].unique().tolist())})"
        )

    def _check_valid_claim_status(self, df: pd.DataFrame) -> None:
        """Status must be one of the known valid values."""
        valid   = {"PAID", "ADJUDICATED", "REVERSED", "PENDING"}
        found   = set(df["claim_status"].unique())
        invalid = found - valid
        self._check(
            "claim_status values valid",
            len(invalid) == 0,
            f"(found: {sorted(list(found))})"
        )

    def _check_service_date_parseable(self, df: pd.DataFrame) -> None:
        """All service dates must be valid parseable dates."""
        parsed     = pd.to_datetime(df["service_date"], errors="coerce")
        null_count = parsed.isna().sum()
        self._check(
            "service_date is parseable",
            null_count == 0,
            f"({null_count} unparseable dates)"
        )