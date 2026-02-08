# assert_strategy.py

from abc import ABC, abstractmethod
from typing import Dict, Any, Tuple, Optional
from functools import reduce
from operator import or_ as or_operator
from pyspark.sql import DataFrame

from datasentinel.conditions import load_conditions
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


class FullOuterJoinStrategy(AssertStrategy):
    """
    Implements a full-recon assert using a full outer join.
    """

    required_attributes = ("join_columns", "compare_columns")

    def assert_(
        self,
        df_a: DataFrame,
        df_b: Optional[DataFrame],
        attributes: dict
    ) -> Dict[str, Any]:
        if df_b is None:
            raise ValueError("full_recon requires RHS dataset.")
        attributes = self.validate(attributes)
        join_cols = attributes.get("join_columns")
        compare_cols = attributes.get("compare_columns")

        a = df_a.alias("a")
        b = df_b.alias("b")

        join_condition = [
            col(f"a.{join_col}") == col(f"b.{join_col}") for join_col in join_cols
        ]

        joined_df = a.join(b, on=join_condition, how="fullouter")

        def select_columns(df: DataFrame) -> DataFrame:
            join_select = [
                coalesce(col(f"a.{join_col}"), col(f"b.{join_col}")).alias(join_col)
                for join_col in join_cols
            ]

            compare_select = [
                col(f"a.{col_name}").alias(f"a_{col_name}")
                for col_name in compare_cols
            ] + [
                col(f"b.{col_name}").alias(f"b_{col_name}")
                for col_name in compare_cols
            ]

            mismatch_select = [
                when(
                    (col(f"a.{col_name}") != col(f"b.{col_name}"))
                    | col(f"a.{col_name}").isNull()
                    | col(f"b.{col_name}").isNull(),
                    1,
                ).otherwise(0).alias(f"{col_name}_mismatch")
                for col_name in compare_cols
            ]

            return df.select(*join_select, *compare_select, *mismatch_select)

        selected_df = select_columns(joined_df)

        mismatch_filters = [
            col(f"{col_name}_mismatch") == 1 for col_name in compare_cols
        ]
        mismatches = selected_df.filter(reduce(or_operator, mismatch_filters))

        a_only = select_columns(
            joined_df.filter(
                col(f"b.{join_cols[0]}").isNull()
                & col(f"a.{join_cols[0]}").isNotNull()
            )
        )
        b_only = select_columns(
            joined_df.filter(
                col(f"a.{join_cols[0]}").isNull()
                & col(f"b.{join_cols[0]}").isNotNull()
            )
        )

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
