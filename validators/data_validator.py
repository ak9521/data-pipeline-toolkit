"""
Data Quality Validator
Production-grade validation utility for DataFrames.
Reusable across any ETL pipeline.
"""

import pandas as pd
import numpy as np
import logging
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    check_name: str
    column: str
    status: str  # "PASS" | "WARN" | "FAIL"
    message: str
    severity: str  # "critical" | "warning" | "info"
    details: Dict[str, Any] = field(default_factory=dict)


class DataValidator:
    """
    Fluent-interface data quality validator.
    Chain validation checks and get a comprehensive quality report.

    Example:
        report = (
            DataValidator(df, name="transactions")
            .check_nulls(["account_id", "amount"], threshold=0.0, severity="critical")
            .check_nulls(["merchant"], threshold=0.05, severity="warning")
            .check_range("amount", min_val=0.01, max_val=1_000_000)
            .check_duplicates(["transaction_id"])
            .check_freshness("timestamp", max_age_hours=24)
            .check_referential_integrity("status", valid_values=["active", "closed", "pending"])
            .report()
        )
    """

    def __init__(self, df: pd.DataFrame, name: str = "dataset"):
        self.df = df.copy()
        self.name = name
        self.results: List[ValidationResult] = []
        self._start_time = datetime.now()

    def check_nulls(
        self,
        columns: List[str],
        threshold: float = 0.0,
        severity: str = "critical"
    ) -> "DataValidator":
        """Check for null values in specified columns."""
        for col in columns:
            if col not in self.df.columns:
                self.results.append(ValidationResult(
                    "null_check", col, "FAIL",
                    f"Column '{col}' not found", severity
                ))
                continue

            null_rate = self.df[col].isnull().mean()
            null_count = self.df[col].isnull().sum()

            if null_rate > threshold:
                status = "FAIL" if severity == "critical" else "WARN"
                msg = f"{null_count:,} nulls ({null_rate:.2%}) — threshold: {threshold:.2%}"
            else:
                status = "PASS"
                msg = f"No nulls above threshold ({null_rate:.2%})"

            self.results.append(ValidationResult(
                "null_check", col, status, msg, severity,
                {"null_count": int(null_count), "null_rate": round(null_rate, 4)}
            ))
        return self

    def check_range(
        self,
        column: str,
        min_val: float = None,
        max_val: float = None,
        severity: str = "critical"
    ) -> "DataValidator":
        """Check numeric column stays within expected bounds."""
        if column not in self.df.columns:
            return self

        violations = pd.Series([False] * len(self.df))
        if min_val is not None:
            violations |= self.df[column] < min_val
        if max_val is not None:
            violations |= self.df[column] > max_val

        vcount = violations.sum()
        status = "PASS" if vcount == 0 else ("FAIL" if severity == "critical" else "WARN")
        range_str = f"[{min_val}, {max_val}]"
        msg = f"{vcount:,} values outside {range_str}" if vcount > 0 else f"All values within {range_str}"

        self.results.append(ValidationResult(
            "range_check", column, status, msg, severity,
            {"violations": int(vcount), "min_val": min_val, "max_val": max_val}
        ))
        return self

    def check_duplicates(
        self,
        key_columns: List[str],
        severity: str = "critical"
    ) -> "DataValidator":
        """Check for duplicate records based on key columns."""
        existing_cols = [c for c in key_columns if c in self.df.columns]
        if not existing_cols:
            return self

        dup_count = self.df.duplicated(subset=existing_cols).sum()
        status = "PASS" if dup_count == 0 else ("FAIL" if severity == "critical" else "WARN")
        msg = f"{dup_count:,} duplicate rows on {existing_cols}" if dup_count > 0 else "No duplicates found"

        self.results.append(ValidationResult(
            "duplicate_check", str(key_columns), status, msg, severity,
            {"duplicate_count": int(dup_count), "key_columns": key_columns}
        ))
        return self

    def check_referential_integrity(
        self,
        column: str,
        valid_values: List[Any],
        severity: str = "warning"
    ) -> "DataValidator":
        """Check column values are within an allowed set."""
        if column not in self.df.columns:
            return self

        invalid = ~self.df[column].isin(valid_values)
        inv_count = invalid.sum()
        inv_vals = self.df.loc[invalid, column].unique().tolist()[:10]
        status = "PASS" if inv_count == 0 else ("FAIL" if severity == "critical" else "WARN")
        msg = f"{inv_count:,} values not in allowed set. Examples: {inv_vals}" if inv_count > 0 else "All values valid"

        self.results.append(ValidationResult(
            "referential_integrity", column, status, msg, severity,
            {"invalid_count": int(inv_count), "invalid_samples": inv_vals}
        ))
        return self

    def check_freshness(
        self,
        timestamp_col: str,
        max_age_hours: int = 24,
        severity: str = "warning"
    ) -> "DataValidator":
        """Check that data is not stale."""
        if timestamp_col not in self.df.columns:
            return self

        latest = pd.to_datetime(self.df[timestamp_col]).max()
        age_hours = (datetime.now() - latest).total_seconds() / 3600
        status = "PASS" if age_hours <= max_age_hours else ("WARN" if severity == "warning" else "FAIL")
        msg = f"Latest record: {latest} ({age_hours:.1f}h ago, limit: {max_age_hours}h)"

        self.results.append(ValidationResult(
            "freshness_check", timestamp_col, status, msg, severity,
            {"latest_timestamp": str(latest), "age_hours": round(age_hours, 2)}
        ))
        return self

    def check_schema_drift(
        self,
        expected_columns: List[str],
        severity: str = "critical"
    ) -> "DataValidator":
        """Detect unexpected schema changes (added/removed columns)."""
        actual = set(self.df.columns)
        expected = set(expected_columns)
        missing = expected - actual
        unexpected = actual - expected

        status = "PASS" if not missing else "FAIL"
        msg = f"Missing: {list(missing)} | Unexpected: {list(unexpected)}" if missing or unexpected else "Schema matches"

        self.results.append(ValidationResult(
            "schema_drift", "all_columns", status, msg, severity,
            {"missing_columns": list(missing), "unexpected_columns": list(unexpected)}
        ))
        return self

    def score(self) -> float:
        """Compute overall data quality score (0–100)."""
        if not self.results:
            return 100.0
        pass_count = sum(1 for r in self.results if r.status == "PASS")
        return round(pass_count / len(self.results) * 100, 1)

    def report(self, verbose: bool = True) -> "DataValidator":
        """Print and return a formatted quality report."""
        score = self.score()
        failures = [r for r in self.results if r.status == "FAIL"]
        warnings = [r for r in self.results if r.status == "WARN"]
        passes = [r for r in self.results if r.status == "PASS"]

        if verbose:
            print(f"\n{'='*60}")
            print(f"  Data Quality Report: {self.name}")
            print(f"  Quality Score: {score}/100  |  {len(passes)} passed, {len(warnings)} warned, {len(failures)} failed")
            print(f"{'─'*60}")
            for r in self.results:
                icon = "✓" if r.status == "PASS" else "⚠" if r.status == "WARN" else "✗"
                print(f"  {icon} [{r.check_name}] {r.column}: {r.message}")
            print(f"{'='*60}\n")

        if failures:
            logger.warning(f"  {len(failures)} critical validation failures detected!")
        return self

    def to_dataframe(self) -> pd.DataFrame:
        """Export results as a DataFrame for logging or storage."""
        return pd.DataFrame([{
            "check_name": r.check_name,
            "column": r.column,
            "status": r.status,
            "message": r.message,
            "severity": r.severity,
        } for r in self.results])
