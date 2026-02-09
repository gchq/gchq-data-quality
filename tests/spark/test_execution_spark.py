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
