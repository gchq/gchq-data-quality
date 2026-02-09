# (c) Crown Copyright GCHQ \n
from pathlib import Path

import pytest

from gchq_data_quality.config import DataQualityConfig
from gchq_data_quality.results.models import DataQualityReport, DataQualityResult


# Most of the config testing in terms of functionality is tested in test_reports.py where we do round-trips, executing rules in the config and recreating it
# here we test exceptions and warnings.
@pytest.fixture
def empty_report() -> DataQualityReport:
    return DataQualityReport(results=[])


def test_from_report_raises_valueerror_on_empty_report(
    empty_report: DataQualityReport,
) -> None:
    with pytest.raises(ValueError, match="DataQualityReport contains no records."):
        DataQualityConfig.from_report(empty_report)


def test_from_report_warns_on_inconsistent_metadata(
    basic_data_quality_result: DataQualityResult,
) -> None:
    alt_result = basic_data_quality_result.model_copy(
        update={"dataset_name": "DIFFERENT"}
    )
    report = DataQualityReport(results=[basic_data_quality_result, alt_result])

    with pytest.warns(UserWarning, match="Inconsistent values for 'dataset_name'"):
        DataQualityConfig.from_report(report)


def test_from_report_warns_if_missing_rule_data(
    basic_data_quality_result: DataQualityResult,
) -> None:
    entry = basic_data_quality_result.model_copy(update={"rule_data": None})
    report = DataQualityReport(results=[entry])

    with pytest.warns(UserWarning, match="No rule_data found for record"):
        config = DataQualityConfig.from_report(report)
        assert config.rules == []


def test_from_report_warns_and_parses_valid_rules(
    basic_data_quality_result: DataQualityResult,
) -> None:
    # First entry: valid rule_data
    valid_entry = basic_data_quality_result
    # Second entry: invalid rule_data (valid JSON, but not valid for creating our rules)
    invalid_entry = basic_data_quality_result.model_copy(
        update={
            "rule_data": '{"field":"id","skip_if_null":"any","data_quality_dimension":"Uniqueness","function":"INVALID"}'
        }
    )
    report = DataQualityReport(results=[valid_entry, invalid_entry])

    with pytest.warns(
        UserWarning, match="Unable to parse rule information from rule_data"
    ):
        config = DataQualityConfig.from_report(report)
        # Only 1 valid rule should be parsed
        assert len(config.rules) == 1


def test_from_report_warns_invalid_json_and_parses_valid_rules(
    basic_data_quality_result: DataQualityResult,
) -> None:
    # First entry: valid rule_data
    valid_entry = basic_data_quality_result
    # Second entry: invalid rule_data (malformed json)
    invalid_entry = basic_data_quality_result.model_copy(
        update={"rule_data": "NOT JSON"}
    )
    report = DataQualityReport(results=[valid_entry, invalid_entry])

    with pytest.warns(
        UserWarning, match="Unable to parse rule information from rule_data"
    ):
        config = DataQualityConfig.from_report(report)
        # Only 1 valid rule should be parsed
        assert len(config.rules) == 1


def test_to_yaml_raises_fileexistserror(tmp_path: Path) -> None:
    config = DataQualityConfig(rules=[], dataset_name="test")
    out_path = tmp_path / "config.yaml"
    # Create file first
    out_path.write_text("existing file")

    with pytest.raises(FileExistsError, match="File already exists"):
        config.to_yaml(out_path, overwrite=False)
