from unittest.mock import Mock

import pytest

from datasentinel.assert_strategy import FullOuterJoinStrategy, SqlAssertStrategy
import datasentinel.executor as executor_module


class _FakeResultDataFrame:
    def __init__(self, columns, rows):
        self.columns = columns
        self._rows = rows

    def limit(self, _n):
        return self

    def collect(self):
        return self._rows


def _fake_df_with_spark(sql_result):
    spark = Mock()
    spark.sql.return_value = sql_result
    df = Mock()
    df.sparkSession = spark
    return df, spark


def test_full_outer_join_strategy_parse_recon_attributes_defaults():
    strategy = FullOuterJoinStrategy()
    join_cols, compare_cols, compare_defs = strategy._parse_recon_attributes(
        {
            "join_columns": ["id"],
            "compare_columns": ["price", "qty"],
        }
    )

    assert join_cols == ["id"]
    assert compare_cols == ["price", "qty"]
    assert compare_defs["price"]["semantics"] == "column_infer"
    assert compare_defs["qty"]["semantics"] == "column_infer"


def test_full_outer_join_strategy_parse_recon_attributes_invalid_compare_columns():
    strategy = FullOuterJoinStrategy()
    with pytest.raises(ValueError, match="compare_columns must be a list or a dict"):
        strategy._parse_recon_attributes(
            {
                "join_columns": ["id"],
                "compare_columns": "price",
            }
        )


def test_sql_assert_strategy_inline_sql_numeric_truthy_passes():
    result_df = _FakeResultDataFrame(["passed"], [[1]])
    df_a, spark = _fake_df_with_spark(result_df)
    df_b = Mock()

    out = SqlAssertStrategy().assert_(df_a, df_b, {"sql": "SELECT 1 AS passed"})

    df_a.createOrReplaceTempView.assert_called_once_with("__LHS__")
    df_b.createOrReplaceTempView.assert_called_once_with("__RHS__")
    spark.sql.assert_called_once_with("SELECT 1 AS passed")
    assert out["status"] == "PASS"
    assert out["dataframes"]["result"] is result_df


def test_sql_assert_strategy_condition_lookup_and_failure_paths(monkeypatch):
    result_df = _FakeResultDataFrame(["passed"], [["no"]])
    df_a, _ = _fake_df_with_spark(result_df)

    monkeypatch.setattr(
        "datasentinel.assert_strategy.load_conditions",
        lambda: {"IS_EMPTY": "SELECT 0 AS passed"},
    )
    out = SqlAssertStrategy().assert_(df_a, None, {"condition_name": "IS_EMPTY"})
    assert out["status"] == "FAIL"

    with pytest.raises(ValueError, match="Unknown condition_name"):
        SqlAssertStrategy().assert_(df_a, None, {"condition_name": "MISSING"})


def test_sql_assert_strategy_requires_single_row_and_single_column():
    strategy = SqlAssertStrategy()

    df_a_cols, _ = _fake_df_with_spark(_FakeResultDataFrame(["a", "b"], [[1, 2]]))
    with pytest.raises(ValueError, match="exactly one column"):
        strategy.assert_(df_a_cols, None, {"sql": "SELECT 1, 2"})

    df_a_rows, _ = _fake_df_with_spark(_FakeResultDataFrame(["passed"], [[True], [False]]))
    with pytest.raises(ValueError, match="exactly one row"):
        strategy.assert_(df_a_rows, None, {"sql": "SELECT * FROM t"})


def test_tester_executor_validation_errors():
    spark = Mock()

    with pytest.raises(ValueError, match="require LHS or LHS_query"):
        executor_module.TesterExecutor(spark, {"type": "test"}, path_resolver=Mock(), run_id="r").execute()

    with pytest.raises(ValueError, match="Provide only one of LHS or LHS_query"):
        executor_module.TesterExecutor(
            spark,
            {"type": "test", "LHS": "a", "LHS_query": "select 1"},
            path_resolver=Mock(),
            run_id="r",
        ).execute()

    with pytest.raises(ValueError, match="Provide only one of RHS or RHS_query"):
        executor_module.TesterExecutor(
            spark,
            {"type": "test", "LHS": "a", "RHS": "b", "RHS_query": "select 1"},
            path_resolver=Mock(),
            run_id="r",
        ).execute()


def test_tester_executor_injects_condition_name_and_uses_table(monkeypatch):
    spark = Mock()
    lhs_df = Mock()
    spark.table.return_value = lhs_df

    strategy = Mock()
    strategy.assert_ = Mock(return_value={"status": "PASS", "dataframes": {}})
    monkeypatch.setattr(
        "datasentinel.executor.StrategyFactory.get_assert_strategy",
        lambda _config: strategy,
    )

    executor = executor_module.TesterExecutor(
        spark=spark,
        config={"type": "test", "name": "t1", "LHS": "lhs", "test": "IS_EMPTY"},
        path_resolver=Mock(),
        run_id="rid",
    )
    out = executor.execute()

    spark.table.assert_called_once_with("lhs")
    spark.sql.assert_not_called()
    strategy.assert_.assert_called_once()
    _, _, attrs = strategy.assert_.call_args[0]
    assert attrs["condition_name"] == "IS_EMPTY"
    assert out["status"] == "PASS"
