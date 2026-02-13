# (c) Crown Copyright GCHQ \n
import pyspark.sql.functions as F  # noqa N812
from pyspark.sql import SparkSession
from gchq_data_quality.spark.utils.rules_utils import (
    get_spark_safe_expression,
    replace_na_values_spark,
)
from gchq_data_quality.spark.utils.rules_utils import get_spark_safe_column_name
from tests.spark.conftest import process_test_data_inputs_for_spark


def test_spark_safe_column_name(spark_safe_column_name_case: dict) -> None:
    result = get_spark_safe_column_name(**spark_safe_column_name_case["inputs"])
    assert result == spark_safe_column_name_case["expected"]["column_name"]


def test_spark_safe_expression(spark_safe_expression_case: dict) -> None:
    result = get_spark_safe_expression(**spark_safe_expression_case["inputs"])
    assert result == spark_safe_expression_case["expected"]["result"]


def test_replace_na_values_spark(
    replace_na_values_case: dict, spark: SparkSession
) -> None:
    test_inputs = process_test_data_inputs_for_spark(
        replace_na_values_case["inputs"], spark
    )
    df = test_inputs.pop("df")

    df_result = replace_na_values_spark(spark_df=df, **test_inputs)

    for col in replace_na_values_case["expected"]:
        assert (
            df_result.filter(F.col(col).isNull()).count()
            == replace_na_values_case["expected"][col]
        )
