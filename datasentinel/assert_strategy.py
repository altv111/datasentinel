# assert_strategy.py

from abc import ABC, abstractmethod
from typing import Dict, Any, Tuple, Optional
from functools import reduce
from operator import or_ as or_operator
from pyspark.sql import DataFrame

from datasentinel.conditions import load_conditions
from datasentinel.native_kernel import run_local_tolerance
from pyspark.sql.functions import col, coalesce, when, abs as sql_abs, max as sql_max


def _spark_null_masks(left_col, right_col):
    both_null = left_col.isNull() & right_col.isNull()
    one_null = left_col.isNull() != right_col.isNull()
    return both_null, one_null


def _string_mismatch_expr(col_a, col_b):
    return ~col_a.eqNullSafe(col_b)


def _numeric_mismatch_expr(col_a, col_b, tolerance):
    col_a_num = col_a.cast("double")
    col_b_num = col_b.cast("double")
    both_null, one_null = _spark_null_masks(col_a, col_b)
    numeric_invalid = (
        (col_a_num.isNull() & col_a.isNotNull())
        | (col_b_num.isNull() & col_b.isNotNull())
    )
    numeric_mismatch = sql_abs(col_a_num - col_b_num) > float(tolerance)
    return when(both_null, False).otherwise(one_null | numeric_invalid | numeric_mismatch)


def _row_infer_mismatch_expr(col_a, col_b, tolerance):
    col_a_num = col_a.cast("double")
    col_b_num = col_b.cast("double")
    both_null, one_null = _spark_null_masks(col_a, col_b)
    numeric_invalid = (
        (col_a_num.isNull() & col_a.isNotNull())
        | (col_b_num.isNull() & col_b.isNotNull())
    )
    numeric_mismatch = sql_abs(col_a_num - col_b_num) > float(tolerance)
    return when(
        both_null,
        False,
    ).otherwise(
        one_null
        | (~numeric_invalid & numeric_mismatch)
        | (numeric_invalid & (col_a != col_b))
    )


def _build_mismatch_expr(col_a, col_b, options, *, non_numeric_flag_col=None):
    options = options or {}
    semantics = options.get("semantics", "column_infer")
    tolerance = options.get("tolerance", 0)
    if semantics == "string":
        return _string_mismatch_expr(col_a, col_b)
    if semantics == "numeric":
        return _numeric_mismatch_expr(col_a, col_b, tolerance)
    if semantics == "row_infer":
        return _row_infer_mismatch_expr(col_a, col_b, tolerance)
    if semantics == "column_infer":
        if non_numeric_flag_col is None:
            raise ValueError("column_infer requires non_numeric_flag_col")
        return when(
            non_numeric_flag_col,
            _string_mismatch_expr(col_a, col_b),
        ).otherwise(_numeric_mismatch_expr(col_a, col_b, tolerance))
    raise ValueError(f"Unknown semantics: {semantics}")


class AssertStrategy(ABC):
    """
    Abstract base class for assert strategies.
    """

    required_attributes: Tuple[str, ...] = ()

    def validate(self, attributes: dict) -> dict:
        missing = [key for key in self.required_attributes if not attributes.get(key)]
        if missing:
            required = ", ".join(missing)
            name = self.__class__.__name__
            raise ValueError(f"{name} requires attributes: {required}.")
        return attributes

    @abstractmethod
    def assert_(
        self,
        df_a: DataFrame,
        df_b: Optional[DataFrame],
        attributes: dict
    ) -> Dict[str, Any]:
        """
        Runs an assertion between two Spark DataFrames.

        Args:
            df_a: The left-hand Spark DataFrame.
            df_b: The right-hand Spark DataFrame, if provided.
            attributes: Strategy-specific configuration.

        Returns:
            A dict with a PASS/FAIL status and a nested dict of dataframes.
        """
        pass


class ReconBaseStrategy(AssertStrategy):
    """
    Shared helpers for recon-style strategies.
    """

    required_attributes = ("join_columns", "compare_columns")

    def _parse_recon_attributes(self, attributes: dict):
        attributes = self.validate(attributes)
        join_cols = attributes.get("join_columns")
        compare_cols = attributes.get("compare_columns")
        if isinstance(compare_cols, dict):
            compare_defs = compare_cols
            compare_cols = list(compare_defs.keys())
        elif isinstance(compare_cols, (list, tuple)):
            compare_defs = {name: {} for name in compare_cols}
            compare_cols = list(compare_cols)
        else:
            raise ValueError("compare_columns must be a list or a dict.")
        for col_name in compare_cols:
            options = compare_defs.get(col_name) or {}
            if "semantics" not in options:
                options["semantics"] = "column_infer"
            compare_defs[col_name] = options
        return join_cols, compare_cols, compare_defs


class FullOuterJoinStrategy(ReconBaseStrategy):
    """
    Implements a full-recon assert using a full outer join.
    """

    def assert_(
        self,
        df_a: DataFrame,
        df_b: Optional[DataFrame],
        attributes: dict
    ) -> Dict[str, Any]:
        if df_b is None:
            raise ValueError("full_recon requires RHS dataset.")
        join_cols, compare_cols, compare_defs = self._parse_recon_attributes(attributes)

        a = df_a.alias("a")
        b = df_b.alias("b")

        join_condition = [
            col(f"a.{join_col}") == col(f"b.{join_col}") for join_col in join_cols
        ]

        joined_df = a.join(b, on=join_condition, how="fullouter")
        needs_column_infer = any(
            (compare_defs.get(col_name) or {}).get("semantics") == "column_infer"
            for col_name in compare_cols
        )
        if needs_column_infer:
            non_numeric_aggs = []
            for col_name in compare_cols:
                options = compare_defs.get(col_name) or {}
                if options.get("semantics") != "column_infer":
                    continue
                col_a = col(f"a.{col_name}")
                col_b = col(f"b.{col_name}")
                non_numeric_expr = (
                    (col_a.cast("double").isNull() & col_a.isNotNull())
                    | (col_b.cast("double").isNull() & col_b.isNotNull())
                )
                non_numeric_aggs.append(
                    sql_max(when(non_numeric_expr, 1).otherwise(0)).alias(
                        f"{col_name}_non_numeric"
                    )
                )
            if non_numeric_aggs:
                flags_df = joined_df.agg(*non_numeric_aggs)
                joined_df = joined_df.crossJoin(flags_df)

        def select_columns(df: DataFrame) -> DataFrame:
            join_select = [
                coalesce(col(f"a.{join_col}"), col(f"b.{join_col}")).alias(join_col)
                for join_col in join_cols
            ]

            compare_select = [
                col(f"a.{col_name}").alias(f"{col_name}_left")
                for col_name in compare_cols
            ] + [
                col(f"b.{col_name}").alias(f"{col_name}_right")
                for col_name in compare_cols
            ]

            mismatch_select = []
            for col_name in compare_cols:
                col_a = col(f"a.{col_name}")
                col_b = col(f"b.{col_name}")
                options = compare_defs.get(col_name) or {}
                non_numeric_flag_col = None
                if options.get("semantics") == "column_infer":
                    non_numeric_flag_col = col(f"{col_name}_non_numeric") == 1
                mismatch_expr = _build_mismatch_expr(
                    col_a,
                    col_b,
                    options,
                    non_numeric_flag_col=non_numeric_flag_col,
                )
                mismatch_select.append(
                    when(mismatch_expr, 1).otherwise(0).alias(f"{col_name}_mismatch")
                )

            return df.select(*join_select, *compare_select, *mismatch_select)

        selected_df = select_columns(joined_df)

        mismatch_filters = [
            col(f"{col_name}_mismatch") == 1 for col_name in compare_cols
        ]
        mismatches = selected_df.filter(reduce(or_operator, mismatch_filters))

        a_present = reduce(or_operator, [col(f"a.{c}").isNotNull() for c in join_cols])
        b_present = reduce(or_operator, [col(f"b.{c}").isNotNull() for c in join_cols])
        a_only = select_columns(joined_df.filter(a_present & ~b_present))
        b_only = select_columns(joined_df.filter(b_present & ~a_present))

        mismatches_empty = mismatches.rdd.isEmpty()
        a_only_empty = a_only.rdd.isEmpty()
        b_only_empty = b_only.rdd.isEmpty()
        status = "PASS" if mismatches_empty and a_only_empty and b_only_empty else "FAIL"

        return {
            "status": status,
            "dataframes": {
                "mismatches": mismatches,
                "a_only": a_only,
                "b_only": b_only,
            },
        }


# Placeholder for an Arrow-backed strategy; currently enforces row-level inference
# using the existing Spark expression path.
class ArrowReconStrategy(FullOuterJoinStrategy):
    def assert_(self, df_a: DataFrame, df_b: Optional[DataFrame], attributes: dict):
        if df_b is None:
            raise ValueError("arrow_recon requires RHS dataset.")
        attrs = dict(attributes) if attributes is not None else {}
        compare_cols = attrs.get("compare_columns")
        if isinstance(compare_cols, dict):
            updated = {}
            for name, options in compare_cols.items():
                options = dict(options or {})
                options["semantics"] = "row_infer"
                updated[name] = options
            attrs["compare_columns"] = updated
        elif isinstance(compare_cols, (list, tuple)):
            attrs["compare_columns"] = {
                name: {"semantics": "row_infer"} for name in compare_cols
            }
        return super().assert_(df_a, df_b, attrs)


# Alias strategy that supports per-column options (e.g., tolerances).
class LocalFastReconStrategy(ReconBaseStrategy):
    def assert_(self, df_a, df_b, config):
        if df_b is None:
            raise ValueError("localfast_recon requires RHS dataset.")
        join_cols, compare_cols, compare_defs = self._parse_recon_attributes(config)
        joined_df = self._join_local(df_a, df_b, join_cols, compare_cols)
        joined_pd = joined_df.toPandas()
        result_col = run_local_tolerance(joined_pd, compare_defs)
        joined_pd["assert_result"] = result_col
        left_present = None
        right_present = None
        for join_col in join_cols:
            left_col = f"{join_col}_left"
            right_col = f"{join_col}_right"
            left_mask = ~joined_pd[left_col].isna()
            right_mask = ~joined_pd[right_col].isna()
            left_present = left_mask if left_present is None else (left_present | left_mask)
            right_present = right_mask if right_present is None else (right_present | right_mask)

        mismatches_pd = joined_pd[left_present & right_present & (~joined_pd["assert_result"])]
        a_only_pd = joined_pd[left_present & (~right_present)]
        b_only_pd = joined_pd[right_present & (~left_present)]

        spark = df_a.sparkSession
        joined_df = spark.createDataFrame(joined_pd)
        schema = joined_df.schema
        mismatches = (
            spark.createDataFrame(mismatches_pd)
            if not mismatches_pd.empty
            else spark.createDataFrame([], schema)
        )
        a_only = (
            spark.createDataFrame(a_only_pd)
            if not a_only_pd.empty
            else spark.createDataFrame([], schema)
        )
        b_only = (
            spark.createDataFrame(b_only_pd)
            if not b_only_pd.empty
            else spark.createDataFrame([], schema)
        )
        status = (
            "PASS"
            if mismatches_pd.empty and a_only_pd.empty and b_only_pd.empty
            else "FAIL"
        )
        return {
            "status": status,
            "dataframes": {
                "mismatches": mismatches,
                "a_only": a_only,
                "b_only": b_only,
            },
        }
    def _join_local(self, df_left, df_right, join_cols, compare_cols):
        a = df_left.alias("a")
        b = df_right.alias("b")
        join_condition = [
            col(f"a.{join_col}") == col(f"b.{join_col}") for join_col in join_cols
        ]
        joined = a.join(b, on=join_condition, how="fullouter")
        join_select = [
            coalesce(col(f"a.{join_col}"), col(f"b.{join_col}")).alias(join_col)
            for join_col in join_cols
        ]
        join_side_select = [
            col(f"a.{join_col}").alias(f"{join_col}_left") for join_col in join_cols
        ] + [
            col(f"b.{join_col}").alias(f"{join_col}_right") for join_col in join_cols
        ]
        compare_select = [
            col(f"a.{col_name}").alias(f"{col_name}_left")
            for col_name in compare_cols
        ] + [
            col(f"b.{col_name}").alias(f"{col_name}_right")
            for col_name in compare_cols
        ]
        # Output includes join keys, join key sides, then compare pairs per column.
        return joined.select(*join_select, *join_side_select, *compare_select)


    


# You can add other comparison strategies here, e.g.,
class CustomStrategy(AssertStrategy):
    def assert_(
        self,
        df_a: DataFrame,
        df_b: Optional[DataFrame],
        attributes: dict
    ) -> Dict[str, Any]:
        # Implement your custom comparison logic here
        raise NotImplementedError("Custom logic comparison not implemented yet")


class SqlAssertStrategy(AssertStrategy):
    """
    Executes a SQL condition that returns a single boolean-like value.
    """

    def assert_(
        self,
        df_a: DataFrame,
        df_b: Optional[DataFrame],
        attributes: dict
    ) -> Dict[str, Any]:
        condition_name = attributes.get("condition_name")
        inline_sql = attributes.get("sql")
        if not condition_name and not inline_sql:
            raise ValueError("sql_assert requires condition_name or sql.")

        df_a.createOrReplaceTempView("__LHS__")
        if df_b is not None:
            df_b.createOrReplaceTempView("__RHS__")

        sql = inline_sql
        if not sql:
            conditions = load_conditions()
            sql = conditions.get(condition_name)
            if not sql:
                raise ValueError(f"Unknown condition_name: {condition_name}")

        spark = df_a.sparkSession
        result_df = spark.sql(sql)
        columns = result_df.columns
        if len(columns) != 1:
            raise ValueError("sql_assert must return exactly one column.")

        rows = result_df.limit(2).collect()
        if len(rows) != 1:
            raise ValueError("sql_assert must return exactly one row.")

        value = rows[0][0]
        if isinstance(value, bool):
            passed = value
        elif isinstance(value, (int, float)):
            passed = value != 0
        elif isinstance(value, str):
            passed = value.strip().lower() in ("true", "t", "1", "yes")
        else:
            raise ValueError("sql_assert returned non-boolean value.")

        return {
            "status": "PASS" if passed else "FAIL",
            "dataframes": {
                "result": result_df,
            },
        }
