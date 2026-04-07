"""
validate.py
Data quality validation using Great Expectations.
Runs checks on raw claims data before it enters the transformation layer.
"""

import pandas as pd
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from src.utils.logger import get_logger

logger = get_logger(__name__)


class ClaimsValidator:
    """Runs a suite of Great Expectations checks on raw claims data."""

    def run_checks(self, df: pd.DataFrame) -> dict:
        """
        Execute all data quality expectations against the input DataFrame.
        Returns the GX validation result dict.
        """
        logger.info(f"Running data quality checks on {len(df):,} rows")

        context = gx.get_context()

        datasource = context.sources.add_or_update_pandas(name="claims_source")
        asset = datasource.add_dataframe_asset(name="raw_claims")

        batch_request = asset.build_batch_request(dataframe=df)

        suite_name = "claims_quality_suite"
        try:
            suite = context.get_expectation_suite(suite_name)
        except Exception:
            suite = context.add_expectation_suite(suite_name)

        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite=suite,
        )

        # ---------------------------------------------------------------- #
        #  Column presence
        # ---------------------------------------------------------------- #
        required_cols = [
            "claim_id", "member_id", "ndc_code", "npi_number",
            "service_date", "claim_amount", "quantity", "days_supply",
            "plan_id", "drug_name", "drug_class", "claim_status",
        ]
        for col in required_cols:
            validator.expect_column_to_exist(col)

        # ---------------------------------------------------------------- #
        #  Null checks
        # ---------------------------------------------------------------- #
        not_null_cols = ["claim_id", "member_id", "ndc_code", "service_date", "claim_amount"]
        for col in not_null_cols:
            validator.expect_column_values_to_not_be_null(col)

        # ---------------------------------------------------------------- #
        #  Uniqueness
        # ---------------------------------------------------------------- #
        validator.expect_column_values_to_be_unique("claim_id")

        # ---------------------------------------------------------------- #
        #  Value ranges
        # ---------------------------------------------------------------- #
        validator.expect_column_values_to_be_between(
            "claim_amount", min_value=0.01, max_value=100_000
        )
        validator.expect_column_values_to_be_between(
            "quantity", min_value=1, max_value=10_000
        )
        validator.expect_column_values_to_be_between(
            "days_supply", min_value=1, max_value=365
        )

        # ---------------------------------------------------------------- #
        #  Categorical values
        # ---------------------------------------------------------------- #
        validator.expect_column_values_to_be_in_set(
            "claim_status", ["PAID", "ADJUDICATED", "REVERSED", "PENDING"]
        )

        # ---------------------------------------------------------------- #
        #  Date format
        # ---------------------------------------------------------------- #
        validator.expect_column_values_to_match_strftime_format(
            "service_date", strftime_format="%Y-%m-%d"
        )

        # ---------------------------------------------------------------- #
        #  Row count sanity check (at least 1000 records expected daily)
        # ---------------------------------------------------------------- #
        validator.expect_table_row_count_to_be_between(
            min_value=1_000, max_value=10_000_000
        )

        # ---------------------------------------------------------------- #
        #  NDC code format (11-digit number)
        # ---------------------------------------------------------------- #
        validator.expect_column_values_to_match_regex(
            "ndc_code", regex=r"^\d{11}$"
        )

        results = validator.validate()

        passed = results["success"]
        total  = len(results["results"])
        failed = sum(1 for r in results["results"] if not r["success"])

        logger.info(
            f"Data quality complete — "
            f"{'PASSED' if passed else 'FAILED'} | "
            f"{total - failed}/{total} checks passed"
        )

        if not passed:
            for r in results["results"]:
                if not r["success"]:
                    logger.error(
                        f"FAILED: {r['expectation_config']['expectation_type']} "
                        f"on column '{r['expectation_config']['kwargs'].get('column', 'N/A')}'"
                    )

        return results.to_json_dict()
