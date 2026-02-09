# (c) Crown Copyright GCHQ \n
import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from gchq_data_quality.spark.dataframe_operations import (
    _create_spark_dataframe,
    _validate_path,
    flatten_spark,
)


def test_flatten_spark(test_nested_df: DataFrame, flatten_spark_case: dict) -> None:
    # our main nested dataframe is defined in spark/conftest.py
    inputs = flatten_spark_case["inputs"]
    expected = flatten_spark_case["expected"]

    flatten_cols = inputs["flatten_cols"]
    # Run flatten_spark on the shared test_nested_df fixture
    flat_df = flatten_spark(test_nested_df, flatten_cols)

    # Row count check
    if "row_count" in expected:
        assert flat_df.count() == expected["row_count"]

    # Column names check (order might not be preserved in spark, so set check)
    if "columns" in expected:
        assert set(flat_df.columns) == set(expected["columns"])

    # Now check expected values for each row as specified in the YAML
    columns_to_check = [col for col in expected["columns"] if col in flat_df.columns]
    flat_pdf = flat_df.toPandas()
    for col in columns_to_check:
        assert len(flat_pdf[col].tolist()) == len(expected[col])
        assert set(flat_pdf[col].tolist()) == set(expected[col])


def test_flatten_spark_raises_on_inconsistent_columns(
    test_nested_df: DataFrame,
) -> None:
    """We don't allow users to explode the same nested columns a different amount i.e. mix [*], []"""
    with pytest.raises(ValueError):
        flatten_spark(
            test_nested_df,
            flatten_cols=["customers.pets[*].name", "customers.pets[].name"],
        )


def test_create_spark_dataframe_case(
    test_nested_df: DataFrame, create_spark_dataframe_case: dict
) -> None:
    """Check overall shape and column names for final spark dataframe that has been
    flattened"""
    expected = create_spark_dataframe_case["expected"]

    # Run the function

    result_df = _create_spark_dataframe(
        test_nested_df, **create_spark_dataframe_case["inputs"]
    )

    # Check output columns match expected columns exactly
    assert len(result_df.columns) == len(expected["columns"])
    assert set(result_df.columns) == set(
        expected["columns"]
    ), f"Expected columns {expected['columns']}, got {result_df.columns}"
    # Row count
    assert result_df.count() == expected["row_count"]


## for testing validating nested paths against schemas
def build_test_schema() -> StructType:
    return StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField(
                "orders",
                ArrayType(
                    StructType(
                        [
                            StructField("date", StringType()),
                            StructField(
                                "items",
                                ArrayType(
                                    StructType(
                                        [
                                            StructField("sku", StringType()),
                                        ]
                                    )
                                ),
                            ),
                        ]
                    )
                ),
            ),
            StructField("notes", ArrayType(StringType())),
            StructField(
                "meta",
                StructType(
                    [
                        StructField("is_active", StringType()),
                    ]
                ),
            ),
        ]
    )


@pytest.mark.parametrize(
    "bad_path, err_match",
    [
        ("notacol", "not found"),
        ("orders", "must end with '\\[\\*]' or '\\[\\]'"),
        ("orders[?]", "not found"),  # invalid array notation
        ("notes.something", "is an array and must end"),  # should be notes[*]
        ("meta.notfound", "not found"),
        ("meta[*]", "not an array"),
        ("notfound[*]", "not found"),
        ("orders[].notreal", "not found"),
        (
            "orders.items[*]",
            "must end with '\\[\\*]' or '\\[\\]'",
        ),  # should be orders[*].item... or orders[].item...
        (
            "orders[*].items",
            "must end with '\\[\\*]' or '\\[\\]'",
        ),  # nested array, no notation for items
    ],
)
def test_validate_path_errors_schema(bad_path: str, err_match: str) -> None:
    schema = build_test_schema()
    with pytest.raises(ValueError, match=err_match):
        _validate_path(schema, bad_path)


def test_validate_path_success_cases() -> None:
    schema = build_test_schema()

    valid_paths = [
        "id",
        "name",
        "orders[*].date",
        "orders[].date",
        "orders[*].items[*].sku",
        "orders[*].items[].sku",
        "notes[]",
        "meta.is_active",
    ]
    for path in valid_paths:
        _validate_path(schema, path)  # should not raise


def test_validate_path_errors_no_schema_passed() -> None:
    with pytest.raises(ValueError):
        _validate_path("Not a Schema", "good.path")  # type: ignore
