# (c) Crown Copyright GCHQ \n
from pyspark.sql import DataFrame

from gchq_data_quality.config import DataQualityConfig
from gchq_data_quality.globals import SampleConfig
from gchq_data_quality.models import DamaFramework
from gchq_data_quality.results.models import DataQualityReport
from gchq_data_quality.rules import ValidityNumericalRangeRule


def test_execute_data_quality_config_spark(
    test_df_spark: DataFrame, config_for_test_df: DataQualityConfig
) -> None:
    # Call the main data quality function with test data and config
    dq_results = config_for_test_df.execute(test_df_spark)

    assert isinstance(dq_results, DataQualityReport)
    # Should be one rule per row
    assert len(dq_results.results) == len(config_for_test_df.rules)


def test_filter_in_rule_spark(test_nested_df: DataFrame) -> None:
    rule = ValidityNumericalRangeRule(
        field="customers.age", filter="`customers.age` < 100", min_value=0, max_value=70
    )
    # we are now prefiltering, the age 105 should not be returned. all should pass (3 out of 3)
    dq_result = rule.evaluate(test_nested_df)
    assert dq_result.records_evaluated == 3
    assert dq_result.pass_rate == 1.0
    assert dq_result.records_passed == 3

    # try filter on columns not being assessed
    rule2 = ValidityNumericalRangeRule(
        field="customers.age",
        filter="`customers.name` != 'Mr No Pets'",
        min_value=0,
        max_value=70,
    )
    # Mr No Pets is 102 years old so result should be the same
    dq_result2 = rule2.evaluate(test_nested_df)
    assert dq_result2.records_evaluated == 3
    assert dq_result2.pass_rate == 1.0


def test_execute_data_quality_config_spark_nested(
    test_nested_df: DataFrame, config_for_nested_data: DataQualityConfig
) -> None:
    # Test main orchestration function with nested data

    dq_results = config_for_nested_data.execute(test_nested_df)

    assert isinstance(dq_results, DataQualityReport)
    # Should be one rule per row
    assert len(dq_results.results) == len(config_for_nested_data.rules)

    dq_results_pdf = dq_results.to_dataframe()

    # All dimensions present
    all_dimensions = set(item.value for item in DamaFramework)
    dimensions_in_report = set(
        dq_results_pdf["data_quality_dimension"].unique().tolist()
    )
    missing = all_dimensions - dimensions_in_report

    assert not missing, f"Missing frameworks in dataframe: {missing}"


def test_change_global_records_failed_sample_size(test_df_spark: DataFrame) -> None:
    SampleConfig.RECORDS_FAILED_SAMPLE_SIZE = 1
    # create a rule we know will fail all values
    rule_4_failures = ValidityNumericalRangeRule(
        field="age", min_value=100
    )  # 4 failures
    result = rule_4_failures.evaluate(test_df_spark)
    assert result.records_failed_sample and len(result.records_failed_sample) == 1
    SampleConfig.RECORDS_FAILED_SAMPLE_SIZE = 10

    result2 = rule_4_failures.evaluate(test_df_spark)

    assert result2.records_failed_sample and len(result2.records_failed_sample) == 4


def test_records_passed_partition_none_handling(test_df_spark: DataFrame) -> None:
    # Take two rows and force two partitions, ensuring one partition records_evaluted = 0
    # We want to ensure that when these partitioned results are aggregated
    # That the correct records_passed values is returned (None + 1) = 1
    df_two_rows = test_df_spark.orderBy("row_number").limit(2)  # rows 1 and 2

    # Filter so only one row is evaluated
    rule = ValidityNumericalRangeRule(
        field="age",
        filter="`row_number` == 1",
        min_value=0,
        max_value=100,
    )

    result = rule.evaluate(df_two_rows)

    # Only one row should have been evaluated
    assert result.records_evaluated == 1

    assert result.records_passed == 1
    assert result.pass_rate == 1.0
