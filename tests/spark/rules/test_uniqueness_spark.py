# (c) Crown Copyright GCHQ \n
# Need to test both the specific spark function
# But also passing params via execute_data_quality_config_spark

from conftest import process_test_data_inputs_for_spark
from pyspark.sql import DataFrame, SparkSession

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


def test_uniqueness_nested_spark(test_nested_df: DataFrame) -> None:
    rule = UniquenessRule(field="customers.name")
    result = rule.evaluate(test_nested_df)
    assert result.records_evaluated == 4
    assert result.pass_rate == 1.0

    # with a filter

    rule2 = UniquenessRule(field="customers.name", filter="`customers.age` < 100")
    result2 = rule2.evaluate(test_nested_df)
    assert result2.records_evaluated == 3
    assert result2.pass_rate == 1.0
