# (c) Crown Copyright GCHQ \n
import json
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from gchq_data_quality.config import DataQualityConfig, RuleType
from gchq_data_quality.models import DamaFramework
from gchq_data_quality.results.models import (
    DataQualityReport,
    DataQualityResult,
)
from gchq_data_quality.rules import (
    AccuracyRule,
    CompletenessRule,
    ConsistencyRule,
    TimelinessRelativeRule,
    TimelinessStaticRule,
    UniquenessRule,
    ValidityNumericalRangeRule,
    ValidityRegexRule,
    # New preferred names
    ValuesAreComplete,
    ValuesAreUnique,
    ValuesMatchExpression,
    ValuesMatchList,
    ValuesMatchNumericalRange,
    ValuesMatchRegex,
    ValuesMatchRelativeTimeBounds,
    ValuesMatchStaticTimeBounds,
)
from tests.conftest import (
    assert_dq_result_matches_expected,
    get_kwargs_for_dq_config,
)

############ ROUND TRIP TESTING ###########

# Some helper functions to
# 1. check DataQualityConfigs are equal, no matter how they are created
# 2. pull out values from our test cases to run the round trips


def assert_configs_equal(
    config1: DataQualityConfig, config2: DataQualityConfig
) -> None:
    """Check each item in a DataQualityConfig is the same, with some exceptions"""

    # Check model type
    assert isinstance(config1, DataQualityConfig)
    assert isinstance(config2, DataQualityConfig)
    # 1. Check the number of rules
    assert len(config1.rules) == len(config2.rules), "Number of rules mismatch"

    # 2. Check each rule in order
    for rule1, rule2 in zip(config1.rules, config2.rules, strict=False):
        assert rule1 == rule2, f"Rule mismatch: {rule1} != {rule2}"

    # 3. Check other model fields, skipping 'rules' and fields where both are None
    config1_dict = config1.model_dump()
    config2_dict = config2.model_dump()

    all_fields = DataQualityConfig.model_fields

    for key in all_fields:
        if key in ["rules", "measurement_time"]:
            continue  # Already checked rules, and measurement_time depends on when execution occured
        config1_value = config1_dict[key]
        config2_value = config2_dict[key]
        # Skip comparison if both are None
        if config1_value is None and config2_value is None:
            continue
        assert (
            config1_value == config2_value
        ), f"Field {key} mismatch: {config1_value} != {config2_value}"


def assert_config_can_save_to_yaml(config: DataQualityConfig, tmp_path: Path) -> None:
    """Helper to save a config to YAML and assert the file exists."""
    yaml_path = Path(tmp_path / "dqconfig_test.yaml")
    config.to_yaml(yaml_path, overwrite=True)
    assert yaml_path.exists()


def validate_report_to_dataframe(report: DataQualityReport) -> None:
    """Check that no errors occur when parsing to_dataframe(), including
    with optional arguments for prettier outputs"""
    num_records = len(report.results)
    df = report.to_dataframe()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == num_records
    assert isinstance(df.loc[0, "measurement_time"], datetime)
    df_pretty = report.to_dataframe(
        decimals=2, measurement_time_format="%Y-%m-%d", records_failed_ids_shift=2
    )
    assert len(df_pretty) == num_records
    assert isinstance(df_pretty, pd.DataFrame)
    assert isinstance(df_pretty.loc[0, "measurement_time"], str)
    assert len(df_pretty) == num_records


def validate_round_trip(
    config: DataQualityConfig, df: pd.DataFrame, expected: list, tmp_path: Path
) -> None:
    """Check we can:
    # 1. import rules
    # 2. run them
    # 3. create a report
    # 4. extract the same data quality config from the report
    # 5. can save the config to YAML
    # 6. can recover those same configs from the YAML file
    # 7. Check the DQ measurements are equivalent in all cases
    # 8. Check the values in the config are equivalent in all cases"""

    assert_config_can_save_to_yaml(config, tmp_path)
    # Execute the rules and check DQ metrics
    report = config.execute(df)
    assert len(report.results) == len(config.rules)
    validate_report_to_dataframe(report)
    for dq_result, exp in zip(report.results, expected, strict=False):
        assert_dq_result_matches_expected(dq_result, exp)

    # Build a config from DQ report and check we get the same answers (should be teh same)
    config_from_report = DataQualityConfig.from_report(report)
    assert_configs_equal(config, config_from_report)
    assert_config_can_save_to_yaml(config_from_report, tmp_path)
    # Check no change in DQ metrics
    report2 = config_from_report.execute(df)
    for dq_result2, exp in zip(report2.results, expected, strict=False):
        assert_dq_result_matches_expected(dq_result2, exp)
    validate_report_to_dataframe(report2)

    # Export config to YAML and reload it - should be the same
    yaml_path = tmp_path / "dqconfig_test.yaml"
    config.to_yaml(yaml_path, overwrite=True)
    config_from_yaml = DataQualityConfig.from_yaml(yaml_path)
    assert_configs_equal(config, config_from_yaml)
    assert_config_can_save_to_yaml(config_from_yaml, tmp_path)

    # Check DQ metrics are the same
    report3 = config_from_yaml.execute(df)
    for dq_result3, exp in zip(report3.results, expected, strict=False):
        assert_dq_result_matches_expected(dq_result3, exp)
    validate_report_to_dataframe(report3)


def get_inputs_for_round_trip(
    test_cases: dict, rule_type: type[RuleType]
) -> tuple[DataQualityConfig, pd.DataFrame, list]:
    """Helper function to:
    1. create the DataQualityConfig from the inputs and the rule type
    2. extract the dataframe with data to measure
    3. extract the DQ metrics to compare against"""

    df = test_cases["inputs"].pop("df")
    inputs = test_cases["inputs"]
    rule = rule_type.model_validate(inputs)
    config_kwargs = get_kwargs_for_dq_config(inputs)
    config = DataQualityConfig(**config_kwargs, rules=[rule])
    # add one top value to the config to ensure this is preserved during round trip
    config.dataset_id = "Round Trip Dataset Name"

    expected = test_cases["expected"]
    if not isinstance(
        expected, list
    ):  # if we just have one set of results it will be a dict
        expected = [expected]
    return config, df, expected


# ---- Legacy name round-trip tests ----


def test_validity_regex_roundtrip(validity_regex_case: dict, tmp_path: Path) -> None:
    """Check all the models interoperate properly YAML <> DataQualityConfig <> DataQualityReport"""
    config, df, expected_results = get_inputs_for_round_trip(
        validity_regex_case, ValidityRegexRule
    )

    validate_round_trip(config, df, expected_results, tmp_path)


def test_validity_numerical_range_roundtrip(
    validity_numerical_range_case: dict, tmp_path: Path
) -> None:
    """Check all the models interoperate properly YAML <> DataQualityConfig <> DataQualityReport"""
    config, df, expected_results = get_inputs_for_round_trip(
        validity_numerical_range_case, ValidityNumericalRangeRule
    )

    validate_round_trip(config, df, expected_results, tmp_path)


def test_completeness_roundtrip(completeness_case: dict, tmp_path: Path) -> None:
    """Check all the models interoperate properly YAML <> DataQualityConfig <> DataQualityReport"""
    config, df, expected_results = get_inputs_for_round_trip(
        completeness_case, CompletenessRule
    )

    validate_round_trip(config, df, expected_results, tmp_path)


def test_consistency_roundtrip(consistency_case: dict, tmp_path: Path) -> None:
    """Check all the models interoperate properly YAML <> DataQualityConfig <> DataQualityReport"""
    config, df, expected_results = get_inputs_for_round_trip(
        consistency_case, ConsistencyRule
    )

    validate_round_trip(config, df, expected_results, tmp_path)


def test_accuracy_roundtrip(accuracy_case: dict, tmp_path: Path) -> None:
    """Check all the models interoperate properly YAML <> DataQualityConfig <> DataQualityReport"""
    config, df, expected_results = get_inputs_for_round_trip(
        accuracy_case, AccuracyRule
    )

    validate_round_trip(config, df, expected_results, tmp_path)


def test_uniqueness_roundtrip(uniqueness_case: dict, tmp_path: Path) -> None:
    """Check all the models interoperate properly YAML <> DataQualityConfig <> DataQualityReport"""
    config, df, expected_results = get_inputs_for_round_trip(
        uniqueness_case, UniquenessRule
    )

    validate_round_trip(config, df, expected_results, tmp_path)


def test_timeliness_static_roundtrip(
    timeliness_static_case: dict, tmp_path: Path
) -> None:
    """Check all the models interoperate properly YAML <> DataQualityConfig <> DataQualityReport"""
    config, df, expected_results = get_inputs_for_round_trip(
        timeliness_static_case, TimelinessStaticRule
    )

    validate_round_trip(config, df, expected_results, tmp_path)


def test_timeliness_relative_roundtrip(
    timeliness_relative_case: dict, tmp_path: Path
) -> None:
    """Check all the models interoperate properly YAML <> DataQualityConfig <> DataQualityReport"""
    config, df, expected_results = get_inputs_for_round_trip(
        timeliness_relative_case, TimelinessRelativeRule
    )

    validate_round_trip(config, df, expected_results, tmp_path)


# ---- New preferred name round-trip tests ----


def test_accuracy_new_name_roundtrip(accuracy_case: dict, tmp_path: Path) -> None:
    """Check ValuesMatchList round-trips properly YAML <> DataQualityConfig <> DataQualityReport"""
    config, df, expected_results = get_inputs_for_round_trip(
        accuracy_case, ValuesMatchList
    )

    validate_round_trip(config, df, expected_results, tmp_path)


def test_values_match_regex_roundtrip(
    validity_regex_case: dict, tmp_path: Path
) -> None:
    """Check ValuesMatchRegex round-trips properly YAML <> DataQualityConfig <> DataQualityReport"""
    config, df, expected_results = get_inputs_for_round_trip(
        validity_regex_case, ValuesMatchRegex
    )

    validate_round_trip(config, df, expected_results, tmp_path)


def test_values_match_numerical_range_roundtrip(
    validity_numerical_range_case: dict, tmp_path: Path
) -> None:
    """Check ValuesMatchNumericalRange round-trips properly YAML <> DataQualityConfig <> DataQualityReport"""
    config, df, expected_results = get_inputs_for_round_trip(
        validity_numerical_range_case, ValuesMatchNumericalRange
    )

    validate_round_trip(config, df, expected_results, tmp_path)


def test_values_are_complete_roundtrip(completeness_case: dict, tmp_path: Path) -> None:
    """Check ValuesAreComplete round-trips properly YAML <> DataQualityConfig <> DataQualityReport"""
    config, df, expected_results = get_inputs_for_round_trip(
        completeness_case, ValuesAreComplete
    )

    validate_round_trip(config, df, expected_results, tmp_path)


def test_values_match_expression_roundtrip(
    consistency_case: dict, tmp_path: Path
) -> None:
    """Check ValuesMatchExpression round-trips properly YAML <> DataQualityConfig <> DataQualityReport"""
    config, df, expected_results = get_inputs_for_round_trip(
        consistency_case, ValuesMatchExpression
    )

    validate_round_trip(config, df, expected_results, tmp_path)


def test_values_are_unique_roundtrip(uniqueness_case: dict, tmp_path: Path) -> None:
    """Check ValuesAreUnique round-trips properly YAML <> DataQualityConfig <> DataQualityReport"""
    config, df, expected_results = get_inputs_for_round_trip(
        uniqueness_case, ValuesAreUnique
    )

    validate_round_trip(config, df, expected_results, tmp_path)


def test_values_match_static_time_bounds_roundtrip(
    timeliness_static_case: dict, tmp_path: Path
) -> None:
    """Check ValuesMatchStaticTimeBounds round-trips properly YAML <> DataQualityConfig <> DataQualityReport"""
    config, df, expected_results = get_inputs_for_round_trip(
        timeliness_static_case, ValuesMatchStaticTimeBounds
    )

    validate_round_trip(config, df, expected_results, tmp_path)


def test_values_match_relative_time_bounds_roundtrip(
    timeliness_relative_case: dict, tmp_path: Path
) -> None:
    """Check ValuesMatchRelativeTimeBounds round-trips properly YAML <> DataQualityConfig <> DataQualityReport"""
    config, df, expected_results = get_inputs_for_round_trip(
        timeliness_relative_case, ValuesMatchRelativeTimeBounds
    )

    validate_round_trip(config, df, expected_results, tmp_path)


# ---- Other result/report tests ----


def test_data_quality_config_created_from_base_rule(tmp_path: Path) -> None:
    """Sometimes we need to pass a rule set List[RuleType] into a DataQualityConfig
    class to create it, rather than a basic dictionary"""

    simple_rule = ValuesAreUnique(field="field_A")
    simple_rule2 = ValuesAreComplete(field="fieldA")

    rules = [simple_rule, simple_rule2]

    dq_config = DataQualityConfig(rules=rules)  # type: ignore - error on List[RuleType] not being same as List[UniquenessRule]
    assert dq_config.rules == rules
    # test saving
    assert_config_can_save_to_yaml(dq_config, tmp_path)


def test_default_measurement_time_behaviour(test_df: pd.DataFrame) -> None:
    # In the config, if we don't set a time, it should be None
    simple_rule = ValuesAreUnique(field="id")
    config = DataQualityConfig(rules=[simple_rule])
    assert config.measurement_time is None
    assert isinstance(config.model_dump_json(), str)

    # If we set a time, it should coerce to UTC Datetime
    config_with_time = DataQualityConfig(
        measurement_time="2025-01-01",  # type: ignore
        rules=[simple_rule],
    )
    assert config_with_time.measurement_time == datetime(2025, 1, 1, tzinfo=UTC)
    assert isinstance(config_with_time.model_dump_json(), str)

    report = config.execute(test_df)
    dq_result = report.results[0]
    dq_result_with_time = config_with_time.execute(test_df).results[0]

    # should default to UTC Now
    assert isinstance(dq_result.measurement_time, datetime)
    assert dq_result.measurement_time > datetime(2025, 1, 1, tzinfo=UTC)
    assert "measurement_time" in dq_result.model_dump()
    # should match the config provided measurement_time
    assert isinstance(dq_result_with_time.measurement_time, datetime)
    assert dq_result_with_time.measurement_time == config_with_time.measurement_time
    assert "measurement_time" in dq_result_with_time.model_dump()

    # If we load from a YAML file, we might set measurement_time as None explicitly

    config_set_time_none = DataQualityConfig(measurement_time=None, rules=[simple_rule])
    assert config_set_time_none.measurement_time is None
    assert "measurement_time" in (config_set_time_none.model_dump())

    # This should default to UTC now
    dq_result_time_as_none = config_set_time_none.execute(test_df).results[0]
    assert isinstance(dq_result_time_as_none.measurement_time, datetime)
    assert dq_result_time_as_none.measurement_time > datetime(2025, 1, 1, tzinfo=UTC)
    assert "measurement_time" in dq_result_time_as_none.model_dump(mode="json")


def test_default_measurement_time_dq_result() -> None:
    dqe = DataQualityResult(
        field="id",
        data_quality_dimension="Consistency",  # type: ignore
        pass_rate=1.0,
        measurement_time=None,  # type: ignore
        rule_data="",
    )
    assert isinstance(dqe.measurement_time, datetime)


def test_set_records_failed_sample_none(
    basic_data_quality_result: DataQualityResult,
) -> None:
    basic_data_quality_result._set_records_failed_sample(None)
    assert basic_data_quality_result.records_failed_sample is None


def test_invalid_measurement_time_format_raises(
    basic_data_quality_report: DataQualityReport,
) -> None:
    with pytest.raises(ValueError) as excinfo:
        _ = basic_data_quality_report.to_dataframe(
            measurement_time_format="NOT_A_REAL_FORMAT"
        )
    assert "Error parsing the measurement_time_format" in str(excinfo.value)


def test_decimals_below_one_raises(
    basic_data_quality_report: DataQualityReport,
) -> None:
    """pass_rate is a float so we should not allow users to round to 0 d.p."""
    with pytest.raises(ValueError) as excinfo:
        _ = basic_data_quality_report.to_dataframe(decimals=0)
    assert "Parameter 'decimals' must be an integer between 1 and 10" in str(
        excinfo.value
    )


def test_warning_on_invalid_records_failed() -> None:
    """We should get a warning, but still get a result if we pass invalid records_failed items"""

    invalid_json = "INVALID"

    with pytest.warns(UserWarning):
        result = DataQualityResult(
            field="id",
            data_quality_dimension=DamaFramework.Completeness,
            pass_rate=0.5,
            rule_data="",
            records_failed_sample=invalid_json,  # type: ignore
        )
        assert result.records_failed_sample is None

    with pytest.warns(UserWarning):
        result = DataQualityResult(
            field="id",
            data_quality_dimension=DamaFramework.Completeness,
            pass_rate=0.5,
            rule_data="",
            records_failed_ids=invalid_json,  # type: ignore
        )
        assert result.records_failed_ids is None


def test_empty_report_outputs_empty_dataframe() -> None:
    empty_report = DataQualityReport(results=[])
    df = empty_report.to_dataframe()
    assert isinstance(df, pd.DataFrame)
    assert df.empty


# ---- __len__ tests ----


def test_len_empty_report() -> None:
    """An empty report has length zero."""
    report = DataQualityReport(results=[])
    assert len(report) == 0


def test_len_report_with_results(basic_data_quality_report: DataQualityReport) -> None:
    """len() matches the number of results stored in the report."""
    assert len(basic_data_quality_report) == len(basic_data_quality_report.results)


def test_len_matches_results_count(
    basic_data_quality_result: DataQualityResult,
) -> None:
    """Explicitly build a report with a known number of results and verify len()."""
    report = DataQualityReport(results=[basic_data_quality_result])
    assert len(report) == 1
    second = basic_data_quality_result.model_copy()
    report2 = DataQualityReport(results=[basic_data_quality_result, second])
    assert len(report2) == 2


# ---- __add__ tests ----


def test_add_two_reports(basic_data_quality_result: DataQualityResult) -> None:
    """Adding two DataQualityReports returns a new report whose length is the sum of both."""
    report_a = DataQualityReport(results=[basic_data_quality_result])
    second = basic_data_quality_result.model_copy()
    report_b = DataQualityReport(results=[second])
    combined = report_a + report_b
    assert isinstance(combined, DataQualityReport)
    assert len(combined) == len(report_a) + len(report_b)
    assert combined.results == report_a.results + report_b.results

    combined_as_list = DataQualityReport() + [basic_data_quality_result, second]
    assert combined_as_list == combined


def test_add_report_does_not_mutate_originals(
    basic_data_quality_result: DataQualityResult,
) -> None:
    """__add__ must not modify either operand."""
    report_a = DataQualityReport(results=[basic_data_quality_result])
    second = basic_data_quality_result.model_copy()
    report_b = DataQualityReport(results=[second])
    original_len_a = len(report_a)
    original_len_b = len(report_b)
    _ = report_a + report_b
    assert len(report_a) == original_len_a
    assert len(report_b) == original_len_b


def test_add_single_result_to_report(
    basic_data_quality_result: DataQualityResult,
) -> None:
    """Adding a single DataQualityResult to a report appends it as one record."""
    report = DataQualityReport(results=[basic_data_quality_result])
    extra = basic_data_quality_result.model_copy()
    combined = report + extra
    assert isinstance(combined, DataQualityReport)
    assert len(combined) == len(report) + 1
    assert combined.results[-1] == extra


def test_add_result_to_empty_report(
    basic_data_quality_result: DataQualityResult,
) -> None:
    """Adding a result to an empty report gives a single-record report."""
    empty = DataQualityReport(results=[])
    combined = empty + basic_data_quality_result
    assert isinstance(combined, DataQualityReport)
    assert len(combined) == 1
    assert combined.results[0] == basic_data_quality_result


def test_add_empty_report_to_report(
    basic_data_quality_result: DataQualityResult,
) -> None:
    """Adding an empty report to a non-empty report preserves all records."""
    report = DataQualityReport(results=[basic_data_quality_result])
    empty = DataQualityReport(results=[])
    combined = report + empty
    assert isinstance(combined, DataQualityReport)
    assert len(combined) == len(report)


def test_add_unsupported_type_returns_not_implemented(
    basic_data_quality_report: DataQualityReport,
    basic_data_quality_result: DataQualityResult,
) -> None:
    """Adding an unsupported type should return NotImplemented (Python will raise TypeError)."""
    result = basic_data_quality_report.__add__("not a report")  # type: ignore[arg-type]
    assert result is NotImplemented

    # Test for adding a list of results where one is invalid
    with pytest.raises(TypeError, match="every item must be a DataQualityResult"):
        result = basic_data_quality_report + [basic_data_quality_result, "not a result"]  # type: ignore


# ---- __iadd__ tests ----


def test_iadd_two_reports(basic_data_quality_result: DataQualityResult) -> None:
    """'+=' with another report extends the original in place and returns the same object."""
    report_a = DataQualityReport(results=[basic_data_quality_result])
    second = basic_data_quality_result.model_copy()
    report_b = DataQualityReport(results=[second])
    original_id = id(report_a)
    original_len_a = len(report_a)
    report_a += report_b
    assert id(report_a) == original_id
    assert len(report_a) == original_len_a + len(report_b)


def test_iadd_single_result(basic_data_quality_result: DataQualityResult) -> None:
    """'+=' with a single DataQualityResult appends it in place."""
    report = DataQualityReport(results=[basic_data_quality_result])
    extra = basic_data_quality_result.model_copy()
    original_id = id(report)
    original_len = len(report)
    report += extra
    assert id(report) == original_id
    assert len(report) == original_len + 1
    assert report.results[-1] == extra


def test_iadd_list_of_results(basic_data_quality_result: DataQualityResult) -> None:
    """'+=' with a list of DataQualityResults extends the report in place."""
    report = DataQualityReport(results=[basic_data_quality_result])
    extra_a = basic_data_quality_result.model_copy()
    extra_b = basic_data_quality_result.model_copy()
    original_len = len(report)
    report += [extra_a, extra_b]
    assert len(report) == original_len + 2


def test_iadd_empty_report_to_report(
    basic_data_quality_result: DataQualityResult,
) -> None:
    """'+=' an empty report leaves the original unchanged."""
    report = DataQualityReport(results=[basic_data_quality_result])
    original_len = len(report)
    report += DataQualityReport(results=[])
    assert len(report) == original_len


def test_iadd_unsupported_type_raises_type_error(
    basic_data_quality_report: DataQualityReport,
) -> None:
    """'+=' with an unsupported type raises TypeError (not NotImplemented)."""
    with pytest.raises(TypeError):
        basic_data_quality_report += "not a report"  # type: ignore[operator]


def test_iadd_list_with_invalid_item_raises_type_error(
    basic_data_quality_report: DataQualityReport,
    basic_data_quality_result: DataQualityResult,
) -> None:
    """'+=' with a list containing a non-result item raises TypeError."""
    with pytest.raises(TypeError):
        basic_data_quality_report += [basic_data_quality_result, "not a result"]  # type: ignore[operator]


def test_to_json_writes_and_roundtrips(
    tmp_path: Path, basic_data_quality_report: DataQualityReport
) -> None:
    json_path = tmp_path / "report.json"
    _ = basic_data_quality_report.to_json(path=str(json_path))

    assert json_path.exists()
    with open(json_path) as f:
        file_contents = f.read()

    # Check roundtrip: json -> DataQualityReport
    reconstituted = DataQualityReport.model_validate_json(file_contents)
    assert isinstance(reconstituted, DataQualityReport)
    assert (
        reconstituted.results[0].model_dump()
        == basic_data_quality_report.results[0].model_dump()
    )


def test_failed_records_outputs(test_df: pd.DataFrame) -> None:
    """We have a range of formats for model_dump and serialisation,
    in particular when in spark, we need to output a JSON string for our records_failed samples
    to accommodate the spark schema"""
    # In the config, if we don't set a time, it should be None
    uniqueness_rule = ValuesAreUnique(field="id")
    dq_result = uniqueness_rule.evaluate(test_df)

    assert isinstance(dq_result, DataQualityResult)
    # Native types with no model dumping
    assert isinstance(dq_result.records_failed_sample, list)
    first_value = dq_result.records_failed_sample[0]
    assert isinstance(first_value, dict)
    assert isinstance(dq_result.records_failed_ids, list)

    python_dump = dq_result.model_dump(mode="python")
    assert isinstance(python_dump["records_failed_sample"], list)
    first_value_python = python_dump["records_failed_sample"][0]
    assert isinstance(first_value_python, dict)
    assert isinstance(python_dump["records_failed_ids"], list)

    spark_schema_df = dq_result._to_spark_schema_df()
    json_sample = spark_schema_df.loc[0, "records_failed_sample"]
    json_ids = spark_schema_df.loc[0, "records_failed_ids"]
    assert isinstance(json_sample, str)
    assert isinstance(json_ids, str)
    # Check these JSON strings are valid
    assert isinstance(json.loads(json_sample), list)
    assert isinstance(json.loads(json_ids), list)

    # Test NaN / NaT in failed record samples can be exported to JSON / dict

    sample_edge_cases = [{"nan": np.nan}, {"nat": pd.NaT}]
    dq_result.records_failed_sample = sample_edge_cases

    dq_result_dump = dq_result.model_dump(mode="json")
    samples_json_dict = dq_result_dump["records_failed_sample"]
    assert len(samples_json_dict) == 2
    assert samples_json_dict[0]["nan"] is None
    assert samples_json_dict[1]["nat"] is None


def test_nan_into_results() -> None:
    # There is some inconsitent behaviour across pyspark and pandas versions whereby a NULL value
    # in Spark can resolve to a NaN in pandas even though the column type should be string

    default_row = {
        "dataset_name": np.nan,
        "dataset_id": np.nan,
        "measurement_sample": np.nan,
        "lifecycle_stage": np.nan,
        "measurement_time": pd.NaT,
        "field": "mandatory",
        "data_quality_dimension": "completeness",
        "records_evaluated": np.nan,
        "records_passed": np.nan,
        "pass_rate": np.nan,
        "rule_id": np.nan,
        "rule_description": np.nan,
        "rule_data": json.dumps({"field": "mandatory"}),
        "records_failed_ids": np.nan,
        "records_failed_sample": np.nan,
    }
    df = pd.DataFrame([default_row])
    result = DataQualityResult.model_validate(df.to_dict(orient="records")[0])
    assert result.measurement_sample is None


def test_wrong_collections_into_results() -> None:
    # If a user tries to pass a list or set into a DataQualityResult e.g. ['MyDataset'] then as we
    # run pd.isna(v) we will get an unhelpful error message around ambiguity as pd.isna(list[str])
    # will return a bool array and therefore not work after an 'if' statement.

    default_row = {
        "dataset_name": [np.nan],
        "dataset_id": np.nan,
        "measurement_sample": np.nan,
        "lifecycle_stage": np.nan,
        "field": "mandatory",
        "data_quality_dimension": "completeness",
        "records_evaluated": np.nan,
        "records_passed": np.nan,
        "pass_rate": np.nan,
        "rule_id": np.nan,
        "rule_description": np.nan,
        "rule_data": json.dumps({"field": "mandatory"}),
        "records_failed_ids": np.nan,
        "records_failed_sample": np.nan,
    }
    df = pd.DataFrame([default_row])
    with pytest.raises(ValueError, match="dataset_name"):
        _ = DataQualityResult.model_validate(df.to_dict(orient="records")[0])
