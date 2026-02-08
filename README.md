# DataSentinel

DataSentinel is a Spark-native framework for **data reconciliation and data quality assertions**
on large-scale datasets.

It is designed for teams that:
- Run PySpark pipelines
- Need system-to-system reconciliation
- Want declarative, YAML-driven data checks
- Prefer lightweight tooling over heavy platforms

## Why DataSentinel?

Most data quality tools are either:
- Too generic
- Too heavy
- Not Spark-first
- Hard to adapt for reconciliation use cases

DataSentinel focuses on **explicit comparisons, deterministic checks,
and clear failure reporting**, while staying close to Spark.

## Key Features
- Spark-native execution
- YAML-based configuration
- Pluggable assert strategies
- CLI-driven execution
- Designed for large datasets

## Quick Start
```bash
datasentinel datasentinel/config.yaml
```

```python
from datasentinel import run, AssertStrategy
```

Environment: `SENTINEL_HOME` sets the base directory for result writes. `SENTINEL_INPUT_HOME` sets the base directory for relative input paths in YAML.

## SQL Asserts
DataSentinel supports SQL-based asserts that evaluate a boolean condition over `LHS` and `RHS`.

Built-in conditions are defined in `datasentinel/conditions.properties`, and you can override or add
your own by creating `$SENTINEL_HOME/conditions.properties`. If a condition appears in both,
the user-defined version wins.

Conditions reference datasets via temp views:
- `__LHS__` for the left-hand dataset
- `__RHS__` for the right-hand dataset (optional)

Example condition entries:
```
IS_EMPTY: SELECT COUNT(*) = 0 AS passed FROM __LHS__
IS_SUBSET_OF: SELECT COUNT(*) = 0 AS passed FROM (SELECT * FROM __LHS__ EXCEPT SELECT * FROM __RHS__)
```

Example YAML:
```yaml
- name: test_is_empty
  type: test
  LHS: datasetA
  test: IS_EMPTY
```

You can also provide queries instead of view names:
```yaml
- name: test_subset
  type: test
  LHS_query: "SELECT * FROM datasetA WHERE active = true"
  RHS_query: "SELECT * FROM datasetB"
  test: IS_SUBSET_OF
```

Note: install with `pip install datasentinel`, import as `datasentinel`.
## Upcoming
- More asserts and built-in SQL conditions
- More loaders 
- Improved CLI
- Concurrent executors (bsed on depends_on in yaml)

