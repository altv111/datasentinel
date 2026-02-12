import pandas as pd
import pytest

from datasentinel.native_kernel import run_local_tolerance


def test_run_local_tolerance_requires_compare_columns():
    df = pd.DataFrame({"a_left": [1], "a_right": [1]})
    with pytest.raises(ValueError, match="No compare columns specified"):
        run_local_tolerance(df, {})


def test_run_local_tolerance_numeric_semantics_with_tolerance():
    df = pd.DataFrame(
        {
            "price_left": [10.0, 10.0, "bad"],
            "price_right": [10.005, 10.03, 10.0],
        }
    )
    out = run_local_tolerance(df, {"price": {"semantics": "numeric", "tolerance": 0.01}})
    assert out.tolist() == [True, False, False]


def test_run_local_tolerance_row_infer_falls_back_to_string_per_row():
    df = pd.DataFrame(
        {
            "value_left": ["10", "abc", "7"],
            "value_right": ["10.0", "abc", "7.2"],
        }
    )
    out = run_local_tolerance(df, {"value": {"semantics": "row_infer", "tolerance": 0.1}})
    assert out.tolist() == [True, True, False]


def test_run_local_tolerance_column_infer_switches_entire_column_to_string():
    df = pd.DataFrame(
        {
            "amount_left": ["10", "abc"],
            "amount_right": ["10.0", "abc"],
        }
    )
    out = run_local_tolerance(df, {"amount": {"semantics": "column_infer", "tolerance": 0.5}})
    assert out.tolist() == [False, True]


def test_run_local_tolerance_null_handling_and_unknown_semantics():
    df = pd.DataFrame(
        {
            "x_left": [None, None, "a"],
            "x_right": [None, "x", None],
        }
    )
    out = run_local_tolerance(df, {"x": {"semantics": "string"}})
    assert out.tolist() == [True, False, False]

    df_unknown = pd.DataFrame({"x_left": ["a"], "x_right": ["b"]})
    with pytest.raises(ValueError, match="Unknown semantics"):
        run_local_tolerance(df_unknown, {"x": {"semantics": "bogus"}})
