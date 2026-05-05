"""
Data Reconciliation Utility
Compares two DataFrames (source vs target) and identifies discrepancies.
Critical for ensuring pipeline integrity in financial systems.
"""

import pandas as pd
import numpy as np
import logging
from dataclasses import dataclass
from typing import List, Optional, Dict

logger = logging.getLogger(__name__)


@dataclass
class ReconciliationReport:
    total_source: int
    total_target: int
    matched: int
    only_in_source: int
    only_in_target: int
    value_mismatches: int
    match_rate: float
    issues: List[Dict]

    def summary(self):
        status = "✓ PASS" if self.match_rate >= 0.99 else "⚠ WARN" if self.match_rate >= 0.95 else "✗ FAIL"
        print(f"\n{'='*55}")
        print(f"  Reconciliation Report  [{status}]")
        print(f"{'─'*55}")
        print(f"  Source records    : {self.total_source:>10,}")
        print(f"  Target records    : {self.total_target:>10,}")
        print(f"  Matched           : {self.matched:>10,}")
        print(f"  Only in source    : {self.only_in_source:>10,}")
        print(f"  Only in target    : {self.only_in_target:>10,}")
        print(f"  Value mismatches  : {self.value_mismatches:>10,}")
        print(f"  Match rate        : {self.match_rate:>10.2%}")
        print(f"{'='*55}\n")


class DataReconciler:
    """
    Compares source and target DataFrames for pipeline reconciliation.
    
    Use cases:
    - Validate ETL pipeline didn't drop or corrupt records
    - Compare raw vs processed counts and aggregates
    - Identify schema or value drift between pipeline stages
    
    Example:
        reconciler = DataReconciler(
            source=raw_df, target=processed_df,
            key_columns=["transaction_id"],
            value_columns=["amount", "account_id"]
        )
        report = reconciler.reconcile()
        report.summary()
    """

    def __init__(
        self,
        source: pd.DataFrame,
        target: pd.DataFrame,
        key_columns: List[str],
        value_columns: Optional[List[str]] = None,
        tolerance: float = 0.001  # for numeric comparisons
    ):
        self.source = source.copy()
        self.target = target.copy()
        self.key_columns = key_columns
        self.value_columns = value_columns or []
        self.tolerance = tolerance

    def reconcile(self) -> ReconciliationReport:
        """Run full reconciliation and return a report."""
        logger.info(f"Reconciling {len(self.source):,} source vs {len(self.target):,} target records...")

        source_keys = set(map(tuple, self.source[self.key_columns].values.tolist()))
        target_keys = set(map(tuple, self.target[self.key_columns].values.tolist()))

        only_source = source_keys - target_keys
        only_target = target_keys - source_keys
        matched_keys = source_keys & target_keys

        # Value comparison for matched keys
        value_mismatches = 0
        issues = []

        if self.value_columns and matched_keys:
            source_indexed = self.source.set_index(self.key_columns)
            target_indexed = self.target.set_index(self.key_columns)

            for key in list(matched_keys)[:1000]:  # sample first 1000 for performance
                for col in self.value_columns:
                    if col not in source_indexed.columns or col not in target_indexed.columns:
                        continue
                    try:
                        s_val = source_indexed.loc[key, col]
                        t_val = target_indexed.loc[key, col]
                        if isinstance(s_val, (int, float)) and isinstance(t_val, (int, float)):
                            if abs(s_val - t_val) > self.tolerance:
                                value_mismatches += 1
                                issues.append({
                                    "key": key, "column": col,
                                    "source_value": s_val, "target_value": t_val,
                                    "diff": abs(s_val - t_val)
                                })
                        elif str(s_val) != str(t_val):
                            value_mismatches += 1
                            issues.append({
                                "key": key, "column": col,
                                "source_value": s_val, "target_value": t_val
                            })
                    except (KeyError, TypeError):
                        continue

        total = max(len(source_keys), len(target_keys))
        match_rate = len(matched_keys) / total if total > 0 else 1.0

        report = ReconciliationReport(
            total_source=len(source_keys),
            total_target=len(target_keys),
            matched=len(matched_keys),
            only_in_source=len(only_source),
            only_in_target=len(only_target),
            value_mismatches=value_mismatches,
            match_rate=match_rate,
            issues=issues[:50]  # cap for readability
        )

        if only_source:
            logger.warning(f"  {len(only_source):,} records in source but NOT in target")
        if only_target:
            logger.warning(f"  {len(only_target):,} records in target but NOT in source")
        if value_mismatches:
            logger.warning(f"  {value_mismatches:,} value mismatches detected")

        logger.info(f"✓ Reconciliation complete — match rate: {match_rate:.2%}")
        return report

    def aggregate_check(self, column: str, agg_func: str = "sum") -> Dict:
        """Compare aggregate values between source and target."""
        agg_fn = {"sum": np.sum, "mean": np.mean, "count": len, "max": np.max, "min": np.min}
        if agg_func not in agg_fn:
            raise ValueError(f"Unsupported aggregation: {agg_func}")

        fn = agg_fn[agg_func]
        src_val = fn(self.source[column].dropna()) if column in self.source.columns else None
        tgt_val = fn(self.target[column].dropna()) if column in self.target.columns else None

        diff = abs(src_val - tgt_val) if src_val and tgt_val else None
        pct_diff = diff / abs(src_val) if src_val and src_val != 0 else None

        result = {
            "column": column, "aggregation": agg_func,
            "source_value": round(src_val, 4) if src_val else None,
            "target_value": round(tgt_val, 4) if tgt_val else None,
            "absolute_diff": round(diff, 4) if diff else None,
            "pct_diff": round(pct_diff * 100, 2) if pct_diff else None,
            "status": "PASS" if pct_diff is not None and pct_diff < 0.01 else "FAIL"
        }

        icon = "✓" if result["status"] == "PASS" else "✗"
        logger.info(
            f"  {icon} {agg_func}({column}): "
            f"source={result['source_value']}, target={result['target_value']}, "
            f"diff={result['pct_diff']}%"
        )
        return result
