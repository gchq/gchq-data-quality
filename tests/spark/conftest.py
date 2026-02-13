# (c) Crown Copyright GCHQ \n
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pyspark.sql.functions as F  # noqa: N812
import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from gchq_data_quality.config import DataQualityConfig
from gchq_data_quality.models import DamaFramework
from gchq_data_quality.rules import (
    AccuracyRule,
    CompletenessRule,
    ConsistencyRule,
    TimelinessRelativeRule,
    TimelinessStaticRule,
    UniquenessRule,
    ValidityNumericalRangeRule,
    ValidityRegexRule,
)
from gchq_data_quality.rules.utils.datetime_utils import to_utc_datetime


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return (
        SparkSession.builder.appName("pytest-pyspark-local-testing")
        .master("local[2]")
        .getOrCreate()
    )


_python_type_to_spark_type = {
    int: IntegerType(),
    float: DoubleType(),
    bool: BooleanType(),
    str: StringType(),
    np.int32: IntegerType(),
    np.int64: IntegerType(),
    np.float32: DoubleType(),
    np.float64: DoubleType(),
    np.bool_: BooleanType(),
    np.str_: StringType(),
}


def inferred_schema_from_non_null(pdf: pd.DataFrame) -> StructType:
    """
    Infers a Spark StructType schema from a Pandas DataFrame, handling columns with nulls and lists.

    This function examines each column in the provided DataFrame and attempts to infer the appropriate Spark
    data type for that column based on its non-null values. Columns containing Python lists are mapped to
    Spark ArrayType with an appropriate elementType determined from the first non-null value in the list.
    Columns containing only null values are mapped to StringType by default.

    Args:
        pdf (pd.DataFrame): The Pandas DataFrame from which to infer the schema.

    Returns:
        StructType: The inferred Spark StructType schema corresponding to the DataFrame columns.

    Raises:
        None.

    Example:
        >>> import pandas as pd
        >>> df = pd.DataFrame({
        ...     'a': [1, 2, None],
        ...     'b': [['x', 'y'], [], None],
        ...     'c': [None, None, None]
        ... })
        >>> schema = inferred_schema_from_non_null(df)
        >>> print(schema)
        StructType([StructField('a', IntegerType(), True),
                    StructField('b', ArrayType(StringType()), True),
                    StructField('c', StringType(), True)])
    """
    fields = []
    for col in pdf.columns:
        non_nulls = pdf[col][pdf[col].notna()].tolist()
        if len(non_nulls) == 0:
            spark_type = StringType()
        else:
            first_value = non_nulls[0]
            spark_type = _python_type_to_spark_type.get(type(first_value))
            if spark_type is None:
                raise ValueError(
                    f"Cannot deterine spark_type from value {first_value} in {non_nulls=}"
                )
        fields.append(StructField(col, spark_type, True))
    return StructType(fields)


def _replace_nan_with_null_in_spark(df: DataFrame) -> DataFrame:
    """
    Replace all NaN values with NULL in a Spark DataFrame.

    Spark treats NaN and NULL as distinct values. This helper normalises
    floating-point NaNs (FloatType / DoubleType) to SQL NULL so that
    downstream logic relying on isNull() behaves consistently.

    This does NOT fix the issue of Pandas 3.0.0 new string dtype NaN values going to NULL,
    this is dealt with separately with the _replace_pandas_string_dtype_with_object function

    Needed as we create Spark dataframes automatically from pandas dataframes
    (via dictionaries) in our test setup, and we compare Spark values (could be NaN / NULL) with those
    parsed from dictionaries (always None - equivalent to NULL)

    Non-floating columns are left unchanged.
    """

    return df.select(
        *[
            F.when(F.isnan(F.col(f.name)), F.lit(None))
            .otherwise(F.col(f.name))
            .alias(f.name)
            if isinstance(f.dataType, (FloatType | DoubleType))
            else F.col(f.name)
            for f in df.schema.fields
        ]
    )


def _replace_pandas_string_dtype_with_object(pdf: pd.DataFrame) -> pd.DataFrame:
    """In Pandas 3.0, columns of string dtype with empty values go to NaN, which Spark
    interrpets as the literal string 'nan', which means our unit tests counting NULL values
    do not count correctly. We can revert away from string dtype to object to solve this.

    We check for string dtype and replace with object dtype

    It is expected this won't impact actual use of the code operationally as creating a spark dataframe
    from a Pandas dataframe is rather unusual, and so the strange NaN > "nan" artifact is unlikely to occur"""

    for col in pdf.columns:
        if pdf[col].dtype == "str":
            pdf[col] = pdf[col].astype(object)

    return pdf


def helper_create_spark_dataframe(
    spark: SparkSession,
    pdf: pd.DataFrame,
    date_col: list | None = None,
    schema: StructType | None = None,
) -> DataFrame:
    """
    Create a Spark DataFrame from a Pandas DataFrame, for the purposes of checking unit tests worked as if they were loaded from a spark dataframe.
    Note that due to differences in how pandas treats NULL values and timezone defaults we need to enforce these in a certain way for these tests. In practice,
    if a native spark dataframe was loaded, it would be more explicitly typed than pandas lets you get away with and this enforcement after the fact wouldn't be needed.

    Parameters:
        pdf (pd.DataFrame): The Pandas DataFrame to convert.
        schema (pyspark.sql.types.StructType, optional): The schema to apply to the Spark DataFrame.
            If not provided, Spark will infer the schema.

    Returns:
        pyspark.sql.DataFrame: The resulting Spark DataFrame.
    """
    # Create a Spark session if not already exists

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    # Replace all NaN/NA values in the DataFrame with None (compatible with Spark)
    pdf = _replace_pandas_string_dtype_with_object(pdf)
    pdf = pdf.where(pd.notnull(pdf), other=None)  # type: ignore , other can be None
    pdf = pdf.replace(
        float("nan"), None
    )  # it seems NaN values are not captured within isnull() this moves everything to NULL

    # Normalize timezone for specified datetime columns
    if date_col:
        for col in date_col:
            if col in pdf.columns:
                # Localize tz-naive datetimes to UTC
                pdf[col] = pdf[col].map(lambda x: to_utc_datetime(x))
    # Handle all-null columns for Spark schema inference (it can't infer a type if all values are null)
    for col in pdf.columns:
        if pdf[col].isnull().all():
            pdf[col] = pdf[col].astype("object")

    if not schema:
        schema = inferred_schema_from_non_null(pdf)

    spark_df = spark.createDataFrame(pdf, schema=schema)
    spark_df = _replace_nan_with_null_in_spark(spark_df)
    return spark_df


def process_test_data_inputs_for_spark(input_dict: dict, spark: SparkSession) -> dict:
    """Handle input test cases for spark. We need to test if there is a 'df'
    key and if there is, return that as a spark dataframe.
    We also infer the schema for that dataframe, based off the non-null values.

    Typically we will use this function:
    process_test_data_inputs(unit_test_case['inputs'])"""

    if "df" in input_dict:
        spark_df = helper_create_spark_dataframe(pdf=input_dict["df"], spark=spark)
        input_dict["df"] = spark_df
    return input_dict


@pytest.fixture(scope="module")
def test_df_spark(spark: SparkSession) -> DataFrame:
    schema = StructType(
        [
            StructField("row_number", IntegerType(), False),  # Add this line
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("email", StringType(), True),
            StructField("category", StringType(), True),
            StructField("score", IntegerType(), True),
            StructField("date", DateType(), True),
        ]
    )

    data = [
        (1, 1, "John", 30, "john@example.com", "A", 10, datetime(2023, 1, 1)),
        (2, 2, "Jane", 25, "jane@example.com", "B", 20, datetime(2023, 2, 1)),
        (3, 3, "Dave", 102, "dave@example", "C", 30, datetime(2023, 3, 1)),
        (4, 3, None, 15, "test@test.com", "D", 40, datetime(2021, 1, 1)),
        (5, 5, "Alice", -5, "alice@example.com", "E", 50, datetime(2023, 5, 1)),
    ]

    return spark.createDataFrame(data, schema)


@pytest.fixture
def config_for_test_df() -> DataQualityConfig:
    """Make sure we run one of each type of rule through the execution function"""
    return DataQualityConfig(
        dataset_name="test_dataset_name",
        measurement_sample="test_sample",
        rules=[
            # Consistency rule: age should be > 18
            ConsistencyRule(field="age", expression="`age` > 18"),
            ConsistencyRule(
                field="age",
                expression={"if": "`age` == 25", "then": '`name` == "Jane"'},
            ),
            # Uniqueness rule: id should be unique
            UniquenessRule(field="id"),
            # Completeness rules
            CompletenessRule(field="name"),
            CompletenessRule(field="id"),
            # ValidityRegexRule: email should match a simple email pattern
            ValidityRegexRule(field="email", regex_pattern=r"[^@]+@[^@]+\.[^@]+"),
            # ValidityNumericalRangeRule: score must be between 0 and 40, inclusive
            ValidityNumericalRangeRule(
                field="score",
                min_value=0,
                max_value=40,
            ),
            # TimelinessStaticRule: date must be after 2022-01-01
            TimelinessStaticRule(
                field="date",
                start_date=datetime(2022, 1, 1),
                end_date=datetime(2023, 3, 1),
            ),
            AccuracyRule(
                field="category",
                valid_values=["A", "B", "C"],
                data_quality_dimension=DamaFramework.Consistency,
            ),
            # TimelinessRelativeRule: date must be within 3 years of today
            TimelinessRelativeRule(
                field="date",
                reference_date=datetime(2023, 1, 1),
                start_timedelta=timedelta(days=0),
                end_timedelta=timedelta(days=365),
            ),
        ],
    )


@pytest.fixture(scope="module")
def test_nested_df(spark: SparkSession) -> DataFrame:
    """Used to test overarching execution functions"""
    data = [
        {
            "id": 1,
            "customers": {
                "name": "John",
                "age": 30,
                "expiry_date": datetime(2025, 12, 31),
                "pets": [
                    {
                        "name": "Fido",
                        "appointments": [
                            {"date": "2022-01-01", "comment": "Fido First appointment"},
                            {
                                "date": "2022-01-02",
                                "comment": "Fido Second appointment",
                            },
                        ],
                    },
                    {
                        "name": "Whiskers",
                        "appointments": [
                            {
                                "date": "2022-02-03",
                                "comment": "Whiskers First appointment",
                            },
                            {
                                "date": "2022-02-04",
                                "comment": "Whiskers Second appointment",
                            },
                        ],
                    },
                ],
            },
        },
        {
            "id": 2,
            "customers": {
                "name": "Jane",
                "age": 25,
                "expiry_date": datetime(2025, 12, 31),
                "pets": [
                    {"name": "Rex", "appointments": []},
                ],
            },
        },
        {
            "id": 3,
            "customers": {
                "name": "Mr No Pets",
                "age": 102,
                "expiry_date": datetime(2025, 12, 31),
                "pets": [
                    {"name": None, "appointments": []},
                ],
            },
        },
        {
            "id": 4,
            "customers": {
                "name": "Mrs Missing Pets",
                "age": 15,
                "expiry_date": datetime(2025, 12, 31),
                "pets": [
                    {
                        "name": "missing",
                        "appointments": [
                            {"date": "2025-01-01", "comment": "none"},
                        ],
                    },
                ],
            },
        },
    ]

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField(
                "customers",
                StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField("age", IntegerType(), True),
                        StructField("expiry_date", DateType(), True),
                        StructField(
                            "pets",
                            ArrayType(
                                StructType(
                                    [
                                        StructField("name", StringType(), True),
                                        StructField(
                                            "appointments",
                                            ArrayType(
                                                StructType(
                                                    [
                                                        StructField(
                                                            "date", StringType(), True
                                                        ),
                                                        StructField(
                                                            "comment",
                                                            StringType(),
                                                            True,
                                                        ),
                                                    ]
                                                )
                                            ),
                                            True,
                                        ),
                                    ]
                                )
                            ),
                            True,
                        ),
                    ]
                ),
                True,
            ),
        ]
    )
    return spark.createDataFrame(data, schema=schema)


@pytest.fixture
def config_for_nested_data() -> DataQualityConfig:
    """Test one of each rule in a nested structure"""
    return DataQualityConfig(
        dataset_name="test_dataset_name",
        dataset_id="01",
        measurement_sample="test_sample",
        lifecycle_stage="ingest",
        rules=[
            ConsistencyRule(
                field="customers", expression="(`customers.age` > 18) or (`id` > 1)"
            ),
            ConsistencyRule(
                field="customers",
                expression={
                    "if": "`customers.pets[*].appointments[*].date`.notnull()",
                    "then": "`customers.pets[*].name`.notnull()",
                },
            ),
            UniquenessRule(field="id"),
            CompletenessRule(field="customers.pets[*].name"),
            CompletenessRule(field="id"),
            AccuracyRule(field="id", valid_values=[1, 2, 3, 4]),
            ValidityRegexRule(field="customers.name", regex_pattern=r"[A-Za-z\s]+"),
            ValidityNumericalRangeRule(
                field="customers.age", min_value=12, max_value=120
            ),
            TimelinessRelativeRule(
                field="customers.pets[*].appointments[*].date",
                reference_column="customers.expiry_date",
                start_timedelta=timedelta(days=-5 * 365),
                end_timedelta=timedelta(days=0),
            ),
            TimelinessStaticRule(
                field="customers.expiry_date",
                start_date=datetime(2022, 1, 1),
                end_date=datetime(2027, 1, 1),
            ),
        ],
    )
