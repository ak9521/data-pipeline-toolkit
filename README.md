# 🛠️ Data Pipeline Toolkit

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-2.x-150458?style=flat-square&logo=pandas&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-blue?style=flat-square)

A library of **production-ready, reusable data quality and reconciliation utilities** designed to plug into any Python ETL pipeline. Built from patterns used in real financial data engineering work.

---

## 📌 What's Inside

| Module | Description |
|--------|-------------|
| `DataValidator` | Fluent-interface validator with 8 check types + quality scoring |
| `DataReconciler` | Source vs target DataFrame comparison with aggregate checks |

---

## 🚀 Quick Start

```python
pip install -r requirements.txt
```

### DataValidator — Validate any DataFrame

```python
from validators.data_validator import DataValidator

report = (
    DataValidator(df, name="transactions")
    .check_nulls(["account_id", "amount"], threshold=0.0, severity="critical")
    .check_nulls(["merchant"], threshold=0.05, severity="warning")
    .check_range("amount", min_val=0.01, max_val=1_000_000)
    .check_duplicates(["transaction_id"])
    .check_freshness("timestamp", max_age_hours=24)
    .check_referential_integrity("status", valid_values=["active", "closed"])
    .check_schema_drift(expected_columns=["account_id", "amount", "timestamp"])
    .report()
)

# → Quality Score: 91.7/100
# → 6 passed, 1 warned, 0 failed
```

### DataReconciler — Compare source vs target

```python
from reconciliation.reconciler import DataReconciler

reconciler = DataReconciler(
    source=raw_df,
    target=processed_df,
    key_columns=["transaction_id"],
    value_columns=["amount"]
)

report = reconciler.reconcile()
report.summary()
# → Match rate: 99.8% ✓ PASS

reconciler.aggregate_check("amount", agg_func="sum")
# → ✓ sum(amount): source=4,823,910.50, target=4,823,910.50, diff=0.00%
```

---

## 📁 Project Structure

```
data-pipeline-toolkit/
├── validators/
│   └── data_validator.py      ← DataValidator with 8 check types
├── reconciliation/
│   └── reconciler.py          ← DataReconciler for source/target comparison
├── tests/
│   └── test_toolkit.py        ← Unit tests
└── requirements.txt
```

---

## ✅ Validation Checks Available

```python
.check_nulls(columns, threshold, severity)           # Null rate enforcement
.check_range(column, min_val, max_val)               # Numeric bounds
.check_duplicates(key_columns)                       # Duplicate detection
.check_referential_integrity(column, valid_values)   # Allowed value sets
.check_freshness(timestamp_col, max_age_hours)       # Data staleness
.check_schema_drift(expected_columns)                # Column drift detection
.score()                                             # Overall quality score 0–100
.to_dataframe()                                      # Export results for logging
```

---
------------------------------------------------------------------------------------------
Output:
D:\Arun_Karthik_GitHub_Portfolio\data-pipeline-toolkit>python -c "import pandas as pd; from validators.data_validator import DataValidator; df = pd.DataFrame({'id':[1,2,3],'amount':[100,200,None]}); DataValidator(df,'test').check_nulls(['amount']).report()"

============================================================
  Data Quality Report: test
  Quality Score: 0.0/100  |  0 passed, 0 warned, 1 failed
────────────────────────────────────────────────────────────
  ✗ [null_check] amount: 1 nulls (33.33%) — threshold: 0.00%
============================================================

  1 critical validation failures detected!
------------------------------------------------------------------------------------------


## 👤 Author

**Arun Karthik Bodduri** — Data Engineer  
[LinkedIn](https://linkedin.com/in/arunkarthikb) · [GitHub](https://github.com/arunkarthikb)
