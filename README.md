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

## Recon Strategy Semantics
DataSentinel provides multiple recon strategies. The two main ones are:
- `FullOuterJoinStrategy` (Spark-native join + per-row comparison)
- `LocalFastReconStrategy` (Spark join + Pandas local tolerance kernel)
- `ArrowReconStrategy` (experimental; row-level inference only; requires native Arrow extension)

They are aligned on null handling for compare columns:
- Both nulls are treated as a match.
- One-side null is a mismatch.

### Comparison semantics (per compare column)
You can control how values are compared using the `semantics` option:
```yaml
compare_columns:
  price:
    tolerance: 0.01
    semantics: column_infer | row_infer | numeric | string
```

Defaults:
- `semantics` defaults to `column_infer`.
- `tolerance` defaults to `0` when omitted.

Modes:
- `column_infer` (default): If any non-numeric value exists in the column (excluding nulls), compare as strings for all rows. Otherwise compare numerically using tolerance.
- `row_infer`: Compare numerically when both values cast to numeric for that row; otherwise compare as strings for that row.
- `numeric`: Always compare numerically; non-numeric values are treated as mismatches.
- `string`: Always compare as strings; tolerance is ignored.

Design notes for the Arrow-backed strategy live in `docs/arrow_recon.md`.
Enable Arrow recon with `DATASENTINEL_ARROW=1` and install the native extension. This is optional;
if you do not use `arrow_recon`, the rest of the package works normally.

## Quick Start
```bash
datasentinel examples/full_recon_test.yaml
```

```python
from datasentinel import run, AssertStrategy
```

Environment: `SENTINEL_HOME` sets the base directory for result writes. `SENTINEL_INPUT_HOME` sets the base directory for relative input paths in YAML.

Loader-specific examples:
- `examples/load_csv_example.yaml`
- `examples/load_json_example.yaml`
- `examples/load_parquet_example.yaml`
- `examples/load_avro_example.yaml`
- `examples/load_jdbc_query_example.yaml`
- `examples/load_jdbc_table_example.yaml`
- `examples/large_recon_multikey_example.yaml` (csv load + transform + SQL assert + multi-key recon on large-ish sample data)

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

## JDBC Loads
Load steps support JDBC sources with either a full table read or a query read.

Required fields for JDBC load steps:
- `format: jdbc`
- `db_type` (supported: `oracle`, `hive`, `postgres`)
- `connection_string`
- exactly one of `table_name` or `query`

Optional fields:
- `driver` (overrides the default driver inferred from `db_type`)
- `jdbc_options` (key/value options passed to Spark JDBC reader, e.g. `fetchsize`)

Example: query-based load
```yaml
- name: trade_pricing_filtered
  type: load
  format: jdbc
  db_type: postgres
  connection_string: jdbc:postgresql://host:5432/riskdb
  query: "SELECT * FROM trade_pricing WHERE trade_date = '2026-02-12' AND tradebook = 'EQD'"
```

Example: full-table load
```yaml
- name: trade_pricing_all
  type: load
  format: jdbc
  db_type: postgres
  connection_string: jdbc:postgresql://host:5432/riskdb
  table_name: trade_pricing
  jdbc_options:
    fetchsize: "1000"
```

Note: install with `pip install datasentinel`, import as `datasentinel`.
## Upcoming
- More asserts and built-in SQL conditions
- More loaders 
- Improved CLI
- Concurrent executors (based on depends_on in yaml)
