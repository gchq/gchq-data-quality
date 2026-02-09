# (c) Crown Copyright GCHQ \n
import pandas as pd
import pytest  # noqa N812

from gchq_data_quality.errors import DQFunctionError
from gchq_data_quality.rules.utils.rules_utils import (
    calculate_pass_rate,
    ensure_columns_exist_pandas,
    evaluate_bool_expression,
    extract_backticked_fields,
    extract_columns_from_expression,
    get_records_failed_ids,
    replace_na_values_pandas,
)
from tests.conftest import (
    EXCEPTION_MAP,
    process_test_data_inputs_for_pandas,
)


def test_extract_columns_from_expression(
    extract_columns_from_expression_case: dict,
) -> None:
    inputs = extract_columns_from_expression_case["inputs"]
    expected = extract_columns_from_expression_case["expected"]

    expression = inputs["expression"]
    df_columns = inputs.get("df_columns", None)

    if "raises" in expected:
        with pytest.raises(EXCEPTION_MAP[expected["raises"]]):
            extract_columns_from_expression(expression, df_columns)
    else:
        result = extract_columns_from_expression(expression, df_columns)
        # Compare sets (order isn't important for unique column output)
        assert set(result) == set(expected["columns"])


def test_get_records_failed_ids(get_records_failed_ids_case: dict) -> None:
    result = get_records_failed_ids(**get_records_failed_ids_case["inputs"])
    assert result == get_records_failed_ids_case["expected"]["records_failed_ids"]


def test_extract_backticked_fields(extract_backticked_fields_case: dict) -> None:
    result = extract_backticked_fields(**extract_backticked_fields_case["inputs"])
    assert result == extract_backticked_fields_case["expected"]["extracted_list"]


def test_calculate_pass_rate__simple_cases() -> None:
    assert calculate_pass_rate(5, 10) == 0.5
    assert calculate_pass_rate(0, 5) == 0.0
    assert calculate_pass_rate(1, 0) is None


def test_ensure_columns_exist_pandas__success() -> None:
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    # Should not raise
    ensure_columns_exist_pandas(df, ["a", "b"])


def test_ensure_columns_exist_pandas__missing() -> None:
    df = pd.DataFrame({"a": [1, 2]})
    with pytest.raises(ValueError):
        ensure_columns_exist_pandas(df, ["a", "b"])


def test_replace_na_values_pandas(replace_na_values_case: dict) -> None:
    """
    Test replace_na_values_pandas for all YAML-driven cases.
    Uses process_test_data_inputs_for_pandas to standardize input creation from case.
    """
    inputs, df = process_test_data_inputs_for_pandas(replace_na_values_case)
    expected = replace_na_values_case["expected"]

    result_df = replace_na_values_pandas(df.copy(), **inputs["inputs"])

    for col, expected_n_null in expected.items():
        actual_nulls = result_df[col].isnull().sum()
        assert actual_nulls == expected_n_null


def test_evaluate_bool_expression_non_boolean_output(test_df: pd.DataFrame) -> None:
    # Expression returns integers, not bools
    non_bool_expression = "`score` + 1"  # produces int output
    with pytest.raises(DQFunctionError, match="does not evaluate to a boolean Series"):
        evaluate_bool_expression(test_df, non_bool_expression)


def test_evaluate_bool_expression_invalid_syntax(test_df: pd.DataFrame) -> None:
    # Expression has invalid syntax
    invalid_expression = "`score` >"
    with pytest.raises(DQFunctionError, match="Error evaluating expression"):
        evaluate_bool_expression(test_df, invalid_expression)


def test_evaluate_bool_expression_missing_column(test_df: pd.DataFrame) -> None:
    # Expression references missing column
    missing_col_expression = "`nonexistent_field` > 2"
    with pytest.raises(DQFunctionError, match="Error evaluating expression"):
        evaluate_bool_expression(test_df, missing_col_expression)
