# (c) Crown Copyright GCHQ \n
"""
Utility functions for pandas-based data quality rule logic.

This module provides helpers for null/NA value handling, evaluating expressions and
column discovery.

Note:
    These utilities are generally called from within rule classes (e.g., Rule.evaluate(df)).
    End users should not call them directly.
"""

import re
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd

from gchq_data_quality.errors import DQFunctionError


def calculate_pass_rate(records_passing: int, records_evaluated: int) -> float | None:
    """
    Safely divide the number of passing records by the total checked records.
    Returns None if records_checked is 0.

    Args:
        passing_records: The number of records passing the rule.
        records_checked: The total number of records checked.

    Returns:
        float: The data quality measure, between 0 and 1.
    """
    if records_evaluated > 0:
        return records_passing / records_evaluated
    return None


def ensure_columns_exist_pandas(df: pd.DataFrame, columns: list[str]) -> None:
    """Raise a ValueError if any requested fields are not present in the Pandas DataFrame columns.

    Args:
        df (pd.DataFrame): The input dataframe.
        columns (list[str]): List of column names to ensure presence.

    Raises:
        ValueError: If any requested column is not in the DataFrame.
    """
    missing = [col for col in columns if col not in df.columns]
    if missing:
        raise ValueError(
            f"Field(s) {missing} not found in DataFrame columns: {df.columns.tolist()}"
        )


def replace_na_values_pandas(
    df: pd.DataFrame,
    columns: list[str],
    na_values: str | int | float | list[Any],
) -> pd.DataFrame:
    """Replace specified values with pd.NA for nullable pandas dtypes.

    Args:
        df (pd.DataFrame): Input dataframe.
        columns (list): List of columns in the dataframe to check.
        na_values (str | int | float | list[Any]): Value or list of values to treat as missing.

    Returns:
        pd.DataFrame: A copy of the input dataframe where the specified values are replaced by pd.NA.
    """
    if not isinstance(na_values, list):
        na_values = [na_values]
    # Always treat None and np.nan as NA, no matter what
    na_values = na_values + [None, np.nan]

    df[columns] = df[columns].replace(na_values, pd.NA)
    return df


def get_records_failed_ids(
    df: pd.DataFrame, failed_mask: pd.Series | np.ndarray | list, max_samples: int = 10
) -> list[int | str | datetime]:
    """Return the row indices of invalid records according to a boolean mask of valid records.

    Args:
        df (pd.DataFrame): The dataframe to check.
        failed_mask (pd.Series | np.ndarray | list): Boolean mask, True meaning record failed and can be exported as a sampe.
        max_samples (int): Maximum number of indices to return.

    Returns:
        list[int]: List of row indices for invalid records, up to max_samples.

    Raises:
        ValueError: If length of mask does not match length of dataframe.
        TypeError: If mask is not a recognised type.
    """

    if isinstance(failed_mask, list):
        failed_mask = np.array(failed_mask)

    if isinstance(failed_mask, np.ndarray):
        failed_mask = pd.Series(failed_mask, index=df.index)
    elif isinstance(failed_mask, pd.Series):
        # Ensure the mask has the same index as the DataFrame
        failed_mask = failed_mask.reindex(df.index)

    # Ensure mask is boolean
    failed_mask = failed_mask.astype(bool)

    # Get invalid index positions
    failed_ids = df.index[failed_mask]

    if len(failed_ids) > 0:
        # Return a list of invalid rows
        return list(failed_ids[:max_samples])
    return []


def evaluate_bool_expression(df: pd.DataFrame, expression: str) -> pd.Series:
    """Evaluate a boolean expression on a DataFrame, returning a nullable boolean Series.

    Args:
        df (pd.DataFrame): The input DataFrame.
        expression (str): Boolean expression to evaluate (e.g. '`a` > 3 & `b` == "higher than 3"').

    Returns:
        pd.Series: A pandas Series of dtype 'boolean', reflecting the result.

    Raises:
        DQFunctionError: If the expression does not evaluate to a boolean Series, or fails to execute.
    """
    try:
        result = df.eval(expression)

        # Ensure result is a pd.Series of boolean dtype
        if not (
            isinstance(result, pd.Series) and pd.api.types.is_bool_dtype(result.dtype)
        ):
            raise DQFunctionError(
                f"Expression '{expression}' does not evaluate to a boolean Series. "
                f"Examples: x > 3, x == True. Returned type is {type(result)}"
            )
        # Convert to pandas nullable BooleanDtype (if not already)
        return pd.Series(result, index=df.index, dtype="boolean")
    except Exception as e:
        raise DQFunctionError(f"Error evaluating expression '{expression}': {e}") from e


def extract_backticked_fields(expression: str) -> list[str]:
    """Extract all substrings wrapped in backticks from an expression.

    Args:
        expression (str): A string possibly containing column references in backticks.

    Returns:
        list[str]: List of columns or field expressions found between backticks.
    """
    return re.findall(r"`([^`]+)`", expression)


def extract_columns_from_expression(
    expression: str, df_columns: list[str] | None = None
) -> list[str]:
    """Extract column names wrapped in backticks from an expression, optionally validating against known columns.

    Args:
        expression (str): The query expression as a string.
        df_columns (list[str] | None): List of column names in the dataframe, or None to skip validation.

    Returns:
        list[str]: List of unique column names found in the expression.

    Raises:
        ValueError: If no columns are found, or if any are missing from df_columns.
    """
    columns = set(extract_backticked_fields(expression))
    if not columns:
        raise ValueError(
            f"No columns found in expression: {expression!r} - are you using backticks (`) around your column names?"
        )

    if df_columns is not None:
        missing = [col for col in columns if col not in df_columns]
        if missing:
            raise ValueError(
                f"Columns {missing} not found in DataFrame columns: {df_columns}"
            )
    return list(columns)
