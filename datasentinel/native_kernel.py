import numpy as np
import pandas as pd
#from datasentinel.native_kernel import compare_with_tolerance


def _null_mask(arr):
    # pd.isna handles mixed/object arrays and None/NaN consistently.
    return pd.isna(arr)


def _null_masks(left_arr, right_arr):
    left_is_null = _null_mask(left_arr)
    right_is_null = _null_mask(right_arr)
    both_null = left_is_null & right_is_null
    one_null = left_is_null ^ right_is_null
    return left_is_null, right_is_null, both_null, one_null


def _coerce_numeric(arr):
    try:
        return arr.astype(float)
    except (TypeError, ValueError):
        return None


def _coerce_numeric_with_invalid(arr, null_mask):
    coerced = pd.to_numeric(arr, errors="coerce")
    coerced_arr = np.asarray(coerced, dtype=float)
    invalid = np.isnan(coerced_arr) & ~null_mask
    return coerced_arr, invalid


def run_local_tolerance(
    joined_df, compare_defs, *, left_suffix="_left", right_suffix="_right"
):
    """
    Run tolerance based comparison on a joined pandas dataframe.
    Returns np.ndarray[bool]: one boolean per row indicating whether all comparisons passed.
    """
    if not compare_defs:
        raise ValueError("No compare columns specified")
    per_column_result = []
    for col, cfg in compare_defs.items():
        tol = cfg.get("tolerance", 0)
        semantics = cfg.get("semantics", "column_infer")
        left_col = f"{col}{left_suffix}"
        right_col = f"{col}{right_suffix}"
        if left_col not in joined_df.columns or right_col not in joined_df.columns:
            raise KeyError(f"Missing column {left_col} or {right_col}")
        left = joined_df[left_col].to_numpy()
        right = joined_df[right_col].to_numpy()
        # null handling
        left_is_null, right_is_null, both_null, one_null = _null_masks(left, right)

        # initialize column result as False
        col_result = np.zeros(len(joined_df), dtype=bool)
        # both nulls -> match
        col_result[both_null] = True
        # both_present -> apply semantics
        both_present = ~(left_is_null | right_is_null)
        if np.any(both_present):
            if semantics == "string":
                col_result[both_present] = left[both_present] == right[both_present]
            elif semantics == "numeric":
                left_num, left_invalid = _coerce_numeric_with_invalid(
                    left[both_present], left_is_null[both_present]
                )
                right_num, right_invalid = _coerce_numeric_with_invalid(
                    right[both_present], right_is_null[both_present]
                )
                numeric_valid = ~(left_invalid | right_invalid)
                numeric_match = np.abs(left_num - right_num) <= tol
                col_result[both_present] = numeric_valid & numeric_match
            elif semantics == "row_infer":
                left_num, left_invalid = _coerce_numeric_with_invalid(
                    left[both_present], left_is_null[both_present]
                )
                right_num, right_invalid = _coerce_numeric_with_invalid(
                    right[both_present], right_is_null[both_present]
                )
                numeric_valid = ~(left_invalid | right_invalid)
                numeric_match = np.abs(left_num - right_num) <= tol
                string_match = left[both_present] == right[both_present]
                col_result[both_present] = (numeric_valid & numeric_match) | (
                    ~numeric_valid & string_match
                )
            elif semantics == "column_infer":
                left_num, left_invalid = _coerce_numeric_with_invalid(
                    left[both_present], left_is_null[both_present]
                )
                right_num, right_invalid = _coerce_numeric_with_invalid(
                    right[both_present], right_is_null[both_present]
                )
                has_non_numeric = np.any(left_invalid | right_invalid)
                if has_non_numeric:
                    col_result[both_present] = left[both_present] == right[both_present]
                else:
                    col_result[both_present] = np.abs(left_num - right_num) <= tol
            else:
                raise ValueError(f"Unknown semantics: {semantics}")
        # one-null rows remain False
        per_column_result.append(col_result)
    return np.logical_and.reduce(per_column_result)

    
                         
