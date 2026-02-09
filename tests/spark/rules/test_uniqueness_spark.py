# (c) Crown Copyright GCHQ \n
# Need to test both the specific spark function
# But also passing params via execute_data_quality_config_spark

from conftest import process_test_data_inputs_for_spark
from pyspark.sql import SparkSession

from gchq_data_quality.rules.uniqueness import UniquenessRule
from tests.conftest import assert_dq_result_matches_expected


def test_uniqueness_spark(uniqueness_case: dict, spark: SparkSession) -> None:
    test_inputs = process_test_data_inputs_for_spark(uniqueness_case["inputs"], spark)
    spark_df = test_inputs.pop("df")
    rule = UniquenessRule(**test_inputs)

    dq_result = rule.evaluate(spark_df)
    assert_dq_result_matches_expected(
        dq_result, uniqueness_case["expected"], ignore_records_failed_ids=True
    )
