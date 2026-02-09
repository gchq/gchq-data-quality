# (c) Crown Copyright GCHQ \n
from conftest import process_test_data_inputs_for_spark
from pyspark.sql import SparkSession

from gchq_data_quality.rules.timeliness import (
    TimelinessRelativeRule,
    TimelinessStaticRule,
)
from tests.conftest import (
    assert_dq_result_matches_expected,
)


def test_timeliness_static_spark(
    timeliness_static_case: dict, spark: SparkSession
) -> None:
    test_inputs = process_test_data_inputs_for_spark(
        timeliness_static_case["inputs"], spark
    )
    spark_df = test_inputs.pop("df")

    # all spark rules other than uniqueness go direct from a config

    rule = TimelinessStaticRule(**test_inputs)  # any extra kwargs are ignored

    dq_result = rule.evaluate(spark_df)
    assert_dq_result_matches_expected(
        dq_result, timeliness_static_case["expected"], ignore_records_failed_ids=True
    )


def test_timeliness_relative_spark(
    timeliness_relative_case: dict, spark: SparkSession
) -> None:
    test_inputs = process_test_data_inputs_for_spark(
        timeliness_relative_case["inputs"], spark
    )
    spark_df = test_inputs.pop("df")

    # all spark rules other than uniqueness go direct from a config

    rule = TimelinessRelativeRule(**test_inputs)  # any extra kwargs are ignored

    dq_result = rule.evaluate(spark_df)
    assert_dq_result_matches_expected(
        dq_result, timeliness_relative_case["expected"], ignore_records_failed_ids=True
    )
