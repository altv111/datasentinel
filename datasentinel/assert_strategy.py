# assert_strategy.py

from abc import ABC, abstractmethod
from typing import Dict, Any, Tuple, Optional
from functools import reduce
from operator import or_ as or_operator
from pyspark.sql import DataFrame

from datasentinel.conditions import load_conditions
from datasentinel.native_kernel import run_local_tolerance
from datasentinel.spark_kernel import run_full_outer_join_recon
from pyspark.sql.functions import col, coalesce, when


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

    @staticmethod
    def _with_counts(attributes: dict) -> bool:
        return bool((attributes or {}).get("summary_with_counts"))

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
        join_cols, compare_cols, compare_defs = self._parse_recon_attributes(attributes)
        out = run_full_outer_join_recon(
            df_a=df_a,
            df_b=df_b,
            join_cols=join_cols,
            compare_cols=compare_cols,
            compare_defs=compare_defs,
        )
        if out.get("status") == "FAIL":
            if self._with_counts(attributes):
                mismatches = out["dataframes"]["mismatches"].count()
                a_only = out["dataframes"]["a_only"].count()
                b_only = out["dataframes"]["b_only"].count()
                out["summary"] = f"mismatches={mismatches}, a_only={a_only}, b_only={b_only}"
            else:
                out["summary"] = "failed with mismatches/a_only/b_only"
        return out


# Placeholder for an Arrow-backed strategy; currently enforces row-level inference
# using the existing Spark expression path.
class ArrowReconStrategy(FullOuterJoinStrategy):
    def assert_(self, df_a: DataFrame, df_b: Optional[DataFrame], attributes: dict):
        import os
        if os.environ.get("DATASENTINEL_ARROW") != "1":
            raise ValueError(
                "ArrowReconStrategy requires DATASENTINEL_ARROW=1 and the "
                "datasentinel_arrow extension installed."
            )
        if df_b is None:
            raise ValueError("arrow_recon requires RHS dataset.")
        from datasentinel.arrow_kernel import build_arrow_compare_udf

        attrs = dict(attributes) if attributes is not None else {}
        join_cols, compare_cols, compare_defs = self._parse_recon_attributes(attrs)
        for name in compare_cols:
            options = compare_defs.get(name) or {}
            options["semantics"] = "row_infer"
            compare_defs[name] = options

        a = df_a.alias("a")
        b = df_b.alias("b")
        join_condition = [
            col(f"a.{join_col}") == col(f"b.{join_col}") for join_col in join_cols
        ]
        joined_df = a.join(b, on=join_condition, how="fullouter")
        compare_udf = build_arrow_compare_udf(compare_cols, compare_defs)
        udf_args = []
        for name in compare_cols:
            udf_args.append(col(f"a.{name}"))
            udf_args.append(col(f"b.{name}"))
        with_matches = joined_df.withColumn("_matches", compare_udf(*udf_args))

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
            mismatch_select = [
                when(~col(f"_matches.{col_name}_match"), 1)
                .otherwise(0)
                .alias(f"{col_name}_mismatch")
                for col_name in compare_cols
            ]
            return df.select(*join_select, *compare_select, *mismatch_select)

        selected_df = select_columns(with_matches)

        mismatch_filters = [
            col(f"{col_name}_mismatch") == 1 for col_name in compare_cols
        ]
        mismatches = selected_df.filter(reduce(or_operator, mismatch_filters))
        a_present = reduce(or_operator, [col(f"a.{c}").isNotNull() for c in join_cols])
        b_present = reduce(or_operator, [col(f"b.{c}").isNotNull() for c in join_cols])
        a_only = select_columns(with_matches.filter(a_present & ~b_present))
        b_only = select_columns(with_matches.filter(b_present & ~a_present))

        mismatches_empty = mismatches.rdd.isEmpty()
        a_only_empty = a_only.rdd.isEmpty()
        b_only_empty = b_only.rdd.isEmpty()
        status = "PASS" if mismatches_empty and a_only_empty and b_only_empty else "FAIL"

        out = {
            "status": status,
            "dataframes": {
                "mismatches": mismatches,
                "a_only": a_only,
                "b_only": b_only,
            },
        }
        if status == "FAIL":
            if self._with_counts(attributes):
                out["summary"] = (
                    f"mismatches={mismatches.count()}, a_only={a_only.count()}, b_only={b_only.count()}"
                )
            else:
                out["summary"] = "failed with mismatches/a_only/b_only"
        return out


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
        out = {
            "status": status,
            "dataframes": {
                "mismatches": mismatches,
                "a_only": a_only,
                "b_only": b_only,
            },
        }
        if status == "FAIL":
            if self._with_counts(config):
                out["summary"] = (
                    f"mismatches={len(mismatches_pd)}, a_only={len(a_only_pd)}, b_only={len(b_only_pd)}"
                )
            else:
                out["summary"] = "failed with mismatches/a_only/b_only"
        return out
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

        out = {
            "status": "PASS" if passed else "FAIL",
            "dataframes": {
                "result": result_df,
            },
        }
        if out["status"] == "FAIL":
            lhs_is_scalar, lhs_value = self._extract_scalar_value(df_a)
            rhs_is_scalar, rhs_value = self._extract_scalar_value(df_b)
            test_name = condition_name or "SQL"
            if lhs_is_scalar and rhs_is_scalar:
                out["summary"] = f"LHS = {lhs_value}, RHS = {rhs_value}, test = {test_name}"
        return out

    @staticmethod
    def _extract_scalar_value(df: Optional[DataFrame]):
        if df is None:
            return False, None
        columns = getattr(df, "columns", None)
        if not isinstance(columns, (list, tuple)) or len(columns) != 1:
            return False, None
        rows = df.limit(2).collect()
        if len(rows) != 1:
            return False, None
        return True, rows[0][0]
