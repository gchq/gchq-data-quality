# (c) Crown Copyright GCHQ \n
"""
Utility functions for handling DataQualityConfig and associated rules in Spark data quality pipelines.

Provides functionality for:
    - Analysing, extracting, and grouping rules according to required fields.
    - Transforming rule and field references to Spark-safe column names, for compatibility with flattened Spark DataFrames.
    - Supporting robust rule execution, including partitioning rules requiring special handling.

"""

import re
from collections.abc import Iterator
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame

from gchq_data_quality.rules.utils.rules_utils import (
    extract_columns_from_expression,
    replace_na_values_pandas,
)


def replace_backticked_with_spark_safe(expression: str, mapping: dict[str, str]) -> str:
    """
    Replaces backticked field references in an expression with their Spark-safe equivalents.

    Args:
        expression: Expression string containing backticked field references.
        mapping: Dictionary mapping original field names to Spark-safe column names.

    Returns:
        str: The modified expression with replaced field references. Each remains backticked.
    """

    def replacer(match: re.Match) -> str:
        original = match.group(1)
        return f"`{mapping.get(original, original)}`"

    return re.sub(r"`([^`]+)`", replacer, expression)


def get_spark_safe_column_name(field: str) -> str:
    """
    Converts a column path containing dots and array markers into a Spark SQL-safe column name.
    Replaces array notation and dots with underscores and descriptive suffixes.

    Args:
        field (str): Path with potential array markers and dot notation.

    Returns:
        str: Spark-safe column name suitable for use in SQL or DataFrame operations.

    Examples:
        "customer.name"         -> "customer_name"
        "orders[*].id"          -> "orders_all_id"
        "items[].cost"          -> "items_first_cost"
        "data.points[*].values[].entry" -> "data_points_all_values_first_entry"
    """
    return (
        field.replace("[*]", "_all")
        .replace("[]", "_first")
        .replace(".", "_")
        .strip("_")
    )


def get_spark_safe_expression(expression: str | dict[str, str]) -> str | dict[str, str]:
    """
    Converts all backticked nested field references in an expression into their Spark-safe equivalents.
    Required because the final expression is executed on a flattened dataframe which will
    have modified column names (modified to be spark safe)

    For example, '`customers[*].children[*].age` > 18 and `orders[].id` == 5' becomes
    '`customers_all_children_all_age` > 18 and `orders_first_id` == 5'.

    Args:
        expression: The input expression possibly containing nested field references.

    Returns:
        str: The expression with nested references replaced by Spark-safe column names.
    """

    def _get_spark_safe_expression_part(expression_part: str) -> str:
        """A spark safe string from an expression part (not a dictionary, just a string)"""
        fields = extract_columns_from_expression(expression_part)
        mapping = {f: get_spark_safe_column_name(f) for f in fields}
        return replace_backticked_with_spark_safe(expression_part, mapping)

    if isinstance(expression, str):
        return _get_spark_safe_expression_part(expression)

    elif isinstance(expression, dict):
        return {
            key: _get_spark_safe_expression_part(expression_part)
            for key, expression_part in expression.items()
        }
    # pydantic will handle other validation errors if this is just passed into self.expression in a rule


def replace_na_values_spark(
    spark_df: SparkDataFrame,
    columns: list,
    na_values: str | int | float | list[Any],
) -> SparkDataFrame:
    """
    Replaces specified values with nulls in a Spark DataFrame in a scalable manner using mapInPandas.

    Args:
        spark_df: The input Spark DataFrame.
        columns: List of columns in which to replace values.
        na_values: Value or list of values to treat as null. May include a mix of types.

    Returns:
        DataFrame: The resulting Spark DataFrame with specified values replaced by null.
    """

    def replace_na_mapper(
        pdf_iter: Iterator[pd.DataFrame],
    ) -> Iterator[pd.DataFrame]:  # pragma: no cover
        for pdf in pdf_iter:
            yield replace_na_values_pandas(pdf, columns, na_values)

    spark_schema = spark_df.schema
    result_spark_df = spark_df.mapInPandas(replace_na_mapper, schema=spark_schema)  # type: ignore
    return result_spark_df
