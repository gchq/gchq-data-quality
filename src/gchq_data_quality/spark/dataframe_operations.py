# (c) Crown Copyright GCHQ \n
"""Utility functions for flattening arrays and nested fields in Spark DataFrames.

Provides sequential flattening logic to transform nested Spark DataFrames
into a single-level, Spark-safe table. Flat tables are required for data quality rule
evaluations.

User does not need to call these functions, however, flatten_spark is a useful diagnostic step
when setting data quality rules within nested data, to see what (flat) dataframe the rule will evaluate against.

Example:
    ```python
    >>> from pyspark.sql import SparkSession
    >>> from gchq_data_quality.spark.dataframe_operations import flatten_spark
    >>> spark = SparkSession.builder.getOrCreate()
    >>> df = ...  # DataFrame with nested columns
    >>> flat_df = flatten_spark(df, ["col[*].subfield", "other.nested"])
    >>> flat_df.show()
    ```
"""

import pyspark.sql.functions as F  # noqa
from pyspark.sql import DataFrame
from pyspark.sql.column import Column
from pyspark.sql.types import ArrayType, StructType, DataType

from gchq_data_quality.spark.utils.rules_utils import (
    get_spark_safe_column_name,
)
from gchq_data_quality.errors import DQFunctionError

# -----------------------------
# Main Public Function
# -----------------------------


def flatten_spark(df: DataFrame, flatten_cols: list[str]) -> DataFrame:
    """
    Flattens arrays and nested fields in a Spark DataFrame to produce a Spark-safe,
    single-level table.

    The columns to flatten may include array or struct paths, with array selections:
        '[*]' - explodes arrays into multiple rows
        '[]' - selects the first non-null element from the array

    Args:
        df: The input Spark DataFrame containing nested or array fields.
        flatten_cols: List of strings indicating nested columns to flatten. Paths may
            include array notation (e.g., 'orders[*].item', 'info.details[]').

    Returns:
        DataFrame: A Spark DataFrame with the specified columns flattened and Spark-safe
        column names.

    Raises:
        ValueError: If the column paths are inconsistent or not found in the schema, or if array notation is misapplied.

    Example:
        Flatten three levels of orders in a customer DataFrame:
        ```python
        flat_df = flatten_spark(df, [
            "customer[*].orders[*].items[*].productId",
            "customer[*].name"
        ])
        flat_df.show()
        ```
    """

    if not any([_is_nested_column(col) for col in flatten_cols]):
        """If no column is nested we don't need to flatten"""
        return df
    # Validate requested paths and prepare explosion order
    nested_paths = _extract_and_sort_nested_paths(flatten_cols)
    if not _nested_paths_are_consistent(nested_paths):
        raise ValueError(
            f"Invalid mix of '[*]' and '[]' at same nesting level: {nested_paths}"
        )

    nested_paths = _rename_nested_paths_for_explosion(nested_paths)

    # Sequentially explode any arrays encountered
    keep_cols = list({_get_parent_column(c) for c in flatten_cols} & set(df.columns))
    df = _explode_arrays_in_order(df, nested_paths, keep_cols)

    # Extract final column names (last explosion / selection)
    final_paths = _final_paths_from_df(flatten_cols)
    df = _extract_final_fields(df, final_paths)

    # Return only requested flattened columns (Spark-safe names)
    safe_final_cols = [get_spark_safe_column_name(c) for c in flatten_cols]
    return df.select(*safe_final_cols)


# -----------------------------
# Second level orchestration functions
# -----------------------------


def _explode_arrays_in_order(
    df: DataFrame, array_paths: list[str], keep_cols: list[str] | None
) -> DataFrame:
    """Explode each array path in order, preserving keep_cols between steps. It is essential
    that array_paths is an ordered list of shallow to deep nested column names."""
    keep_cols = keep_cols or []
    for path in array_paths:
        df = _create_spark_dataframe(df, path, keep_cols)
        keep_cols = list(set(keep_cols) | set(df.columns))
    return df


def _extract_final_fields(df: DataFrame, field_paths: list[str]) -> DataFrame:
    """Extract the final fields after explosions,
    keeping all intermediate columns except the one being replaced (field).

    This is called as the last explosion step."""
    for field in field_paths:
        df = _create_spark_dataframe(
            df, field, keep_cols=list(set(df.columns) - {field})
        )
    return df


# -----------------------------
# Array Operations (exploding arrays and getting all values, or the first value)
# -----------------------------


def _is_nested_column(column: str) -> bool:
    """Tests if we have a nested column structure"""
    if _is_array_column_name(column) or ("." in column):
        return True
    else:
        return False


def _explode_array(
    df: DataFrame, col_ref: str, alias_name: str, keep_cols: list[str] | None
) -> DataFrame:
    """Explodes an array column into multiple rows, keeping specified columns."""
    return _with_added_column(
        df, keep_cols, F.explode_outer(F.col(col_ref)), alias_name
    )


def _pick_first_from_array(
    df: DataFrame, col_ref: str, alias_name: str, keep_cols: list[str] | None
) -> DataFrame:
    """Selects the first non-null element from an array column, retaining any given columns."""
    filter_expr = F.expr(f"get(filter({col_ref}, x -> x IS NOT NULL), 0)")
    return _with_added_column(df, keep_cols, filter_expr, alias_name)


def _select_field(
    df: DataFrame, col_ref: str, alias_name: str, keep_cols: list[str] | None
) -> DataFrame:
    """Selects a field by column reference and assigns an alias, keeping other columns as specified."""
    return _with_added_column(df, keep_cols, F.col(col_ref), alias_name)


def _apply_array_operation(
    df: DataFrame,
    col_ref: str,
    alias_name: str,
    notation: str,
    keep_cols: list[str] | None = None,
) -> DataFrame:
    """Dispatch the correct transformation based on array notation."""
    if notation == "[*]":
        return _explode_array(df, col_ref, alias_name, keep_cols)
    elif notation == "[]":
        return _pick_first_from_array(df, col_ref, alias_name, keep_cols)
    else:
        return _select_field(df, col_ref, alias_name, keep_cols)


# -----------------------------
# Primary 'unit' of flattening logic on a dataframe
# This will explode and flatten 'field', whilst keeping keep_cols
# -----------------------------


def _create_spark_dataframe(
    df: DataFrame, field: str, keep_cols: list[str] | None = None
) -> DataFrame:
    """Extracts and flattens the given field from df, whilst keeping keep_cols, which will be needed
    for subsequent explosion operations"""
    keep_cols = [get_spark_safe_column_name(col) for col in (keep_cols or [])]

    _validate_path(df.schema, field)  # will raise ValueError if invalid

    alias_name = None
    current_df = df

    for part in field.split("."):
        base_name, notation = _split_array_notation(part)
        col_ref = base_name if alias_name is None else f"{alias_name}.{base_name}"

        alias_name = get_spark_safe_column_name(
            f"{alias_name}.{part}" if alias_name else part
        )

        select_keep_cols = _safe_keep_cols(current_df, keep_cols, exclude=alias_name)
        current_df = _apply_array_operation(
            current_df, col_ref, alias_name, notation, select_keep_cols
        )

    final_column_name = get_spark_safe_column_name(field)
    if alias_name is None:
        raise DQFunctionError(
            f"Something unexpected has occurred. alias_name is None, perhaps a column with no nested structure has been passed in? {field=}, {keep_cols=}. \
                              Pydantic validations should ensure this never raises."
        )
    return current_df.select(
        *_safe_keep_cols(current_df, keep_cols),
        F.col(alias_name).alias(final_column_name),
    )


# -----------------------------
# Validation of nested fields / columns
# -----------------------------
def _validate_path(schema: StructType, path: str) -> None:
    """Raises ValueError if path does not match DataFrame schema or notation rules."""

    def get_struct_field(schema: StructType | DataType, field_name: str) -> DataType:
        """Return DataType of field in StructType; raises ValueError with path context if missing or invalid.
        schema should only be a StructType in the validate path logic"""
        if not isinstance(schema, StructType):
            raise ValueError(
                f"Schema is not StructType when accessing field '{field_name}' of path '{path}'."
            )
        if field_name not in schema.fieldNames():
            raise ValueError(
                f"Column '{field_name}' in path '{path}' not found. Available fields: {schema.fieldNames()}"
            )
        return schema[field_name].dataType

    cols = path.split(".")
    current_schema = schema

    for col_name in cols:
        if _is_array_column_name(col_name):
            base_col, _ = _split_array_notation(col_name)
            field_type = get_struct_field(current_schema, base_col)
            if not isinstance(field_type, ArrayType):
                raise ValueError(
                    f"Column '{base_col}' in path '{path}' is not an array."
                )
            current_schema = field_type.elementType
        else:
            field_type = get_struct_field(current_schema, col_name)
            if isinstance(field_type, ArrayType):
                raise ValueError(
                    f"Column '{col_name}' in path '{path}' is an array and must end with '[*]' or '[]'."
                )
            if isinstance(field_type, StructType):
                current_schema = field_type


# -----------------------------
# Nested column names sorting and discover
# -----------------------------


def _is_array_column_name(col_name: str) -> bool:
    return col_name.endswith("[*]") or col_name.endswith("[]")


def _split_array_notation(part: str) -> tuple[str, str]:
    """Splits a column part to its base name and array notation, if any.

    Examples:
        >>> _split_array_notation("orders[*]")
        ('orders', '[*]')
        >>> _split_array_notation("items[]")
        ('items', '[]')
        >>> _split_array_notation("customer")
        ('customer', '')
    """
    if part.endswith("[*]"):
        return part[:-3], "[*]"
    elif part.endswith("[]"):
        return part[:-2], "[]"
    else:
        return part, ""


def _safe_keep_cols(
    df: DataFrame, keep_cols: list[str] | None, exclude: str | None = None
) -> list[str]:
    """Filter keep_cols to those present in df, optionally excluding one."""
    valid = [c for c in (keep_cols or []) if c in df.columns]
    return [c for c in valid if c != exclude] if exclude else valid


def _with_added_column(
    df: DataFrame, keep_cols: list[str] | None, col_expr: Column, alias: str
) -> DataFrame:
    """Adds an additional column to the dataframe, which is created using col_expr, but will have the name alias."""
    return df.select(*_safe_keep_cols(df, keep_cols), col_expr.alias(alias))


def _get_parent_column(nested_col_name: str) -> str:
    """The 'ancestor' of a nested column name (the string prefix before the first full stop)
    one.two.three.four > one"""
    base, _ = _split_array_notation(nested_col_name.split(".")[0])
    return base


def _extract_and_sort_nested_paths(flatten_cols: list[str]) -> list[str]:
    """Sorts the nested path column list from shallow to deep.
    e.g. 'name' > 'name.age' > 'name.orders[*].date.

    This is because when sequentially exploding a nested data structure, we need to start shallow"""
    nested_paths = set()
    for col_path in flatten_cols:
        parts = col_path.split(".")
        for i in range(len(parts)):
            if _is_array_column_name(parts[i]):
                nested_paths.add(".".join(parts[: i + 1]))
    return sorted(nested_paths, key=lambda p: (p.count("."), p))


def _nested_paths_are_consistent(array_paths: list) -> bool:
    """True if no array level mixes '[*]' and '[]' notation.
    It does this by counting each string preceding [*] and [], the same
    string prefix should not have both [*] and [] as notations."""
    level_notations = {}
    for path in array_paths:
        parts = path.split(".")
        for i, part in enumerate(parts):
            if _is_array_column_name(part):
                base, notation = _split_array_notation(part)
                prefix = ".".join(parts[:i] + [base])
                level_notations.setdefault(prefix, set()).add(notation)
                if len(level_notations[prefix]) > 1:
                    return False
    return True


def _rename_nested_paths_for_explosion(array_paths: list) -> list:
    """Renames each nested column name with the final column that will be required just
    before explosion of an array, or extracting a nested value.

    e.g. [one, one.two[*].three] > [one, one_two_all.three]
    As just before getting 'three' we will have exploded all levels of one.two"""
    if not array_paths:
        return []
    new_array = [array_paths[0]]
    for i in range(1, len(array_paths)):
        prev_original = array_paths[i - 1]
        prev_safe = get_spark_safe_column_name(new_array[i - 1])
        if array_paths[i].startswith(prev_original + "."):
            rest = array_paths[i][len(prev_original) + 1 :]
            name = f"{prev_safe}.{rest}"
        else:
            name = array_paths[i].replace(prev_original, prev_safe, 1)
        new_array.append(name)
    return new_array


def _final_paths_from_df(flatten_cols: list[str]) -> list[str]:
    """The final column names required in the completely flattened dataframe"""
    final_paths = []
    for col in flatten_cols:
        final_part = col.split("]")[-1]
        if final_part == "":
            first_part = get_spark_safe_column_name(col)
        else:
            first_part = col.rsplit(final_part, 1)[0]
            first_part = get_spark_safe_column_name(first_part)
        final_paths.append(first_part + final_part)
    return final_paths
