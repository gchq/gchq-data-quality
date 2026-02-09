# (c) Crown Copyright GCHQ \n
from conftest import process_test_data_inputs_for_spark
from pyspark.sql import SparkSession

from gchq_data_quality.rules.accuracy import AccuracyRule
from tests.conftest import (
    assert_dq_result_matches_expected,
)


def test_accuracy_spark(accuracy_case: dict, spark: SparkSession) -> None:
    test_inputs = process_test_data_inputs_for_spark(accuracy_case["inputs"], spark)
    spark_df = test_inputs.pop("df")

    # all spark rules other than uniqueness go direct from a config

    rule = AccuracyRule(**test_inputs)  # any extra kwargs are ignored
    dq_result = rule.evaluate(spark_df)

    assert_dq_result_matches_expected(
        dq_result, accuracy_case["expected"], ignore_records_failed_ids=True
    )
