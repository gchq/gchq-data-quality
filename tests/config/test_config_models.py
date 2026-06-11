# (c) Crown Copyright GCHQ \n
from pathlib import Path

import pytest

from gchq_data_quality.config import DataQualityConfig
from gchq_data_quality.results.models import DataQualityReport, DataQualityResult
from gchq_data_quality.rules.completeness import ValuesAreComplete
from gchq_data_quality.rules.uniqueness import ValuesAreUnique

# Most of the config testing in terms of functionality is tested in test_reports.py where we do round-trips, executing rules in the config and recreating it
# here we test exceptions and warnings.


@pytest.fixture
def simple_rule() -> ValuesAreComplete:
    return ValuesAreComplete(field="test_field")


@pytest.fixture
def another_rule() -> ValuesAreUnique:
    return ValuesAreUnique(field="id")


@pytest.fixture
def basic_config(simple_rule: ValuesAreComplete) -> DataQualityConfig:
    return DataQualityConfig(dataset_name="test_dataset", rules=[simple_rule])


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


# ---- __len__ tests ----


def test_len_empty_config() -> None:
    """An empty config has length 0."""
    config = DataQualityConfig()
    assert len(config) == 0


def test_len_single_rule(simple_rule: ValuesAreComplete) -> None:
    """A config with one rule has length 1."""
    config = DataQualityConfig(rules=[simple_rule])
    assert len(config) == 1


def test_len_multiple_rules(
    simple_rule: ValuesAreComplete, another_rule: ValuesAreUnique
) -> None:
    """Length reflects the exact number of rules present."""
    config = DataQualityConfig(rules=[simple_rule, another_rule])
    assert len(config) == 2


# ---- __add__ tests ----


def test_add_single_rule_to_config(
    basic_config: DataQualityConfig, another_rule: ValuesAreUnique
) -> None:
    """Adding a single rule returns a new config with that rule appended."""
    combined = basic_config + another_rule
    assert isinstance(combined, DataQualityConfig)
    assert len(combined) == len(basic_config) + 1
    assert combined.rules[-1] == another_rule


def test_add_list_of_rules_to_config(
    basic_config: DataQualityConfig,
    simple_rule: ValuesAreComplete,
    another_rule: ValuesAreUnique,
) -> None:
    """Adding a list of rules returns a new config with all rules appended."""
    combined = basic_config + [simple_rule, another_rule]  # type: ignore[operator]
    assert isinstance(combined, DataQualityConfig)
    assert len(combined) == len(basic_config) + 2


def test_add_rule_to_empty_config(simple_rule: ValuesAreComplete) -> None:
    """Adding a rule to an empty config produces a single-rule config."""
    empty = DataQualityConfig()
    combined = empty + simple_rule
    assert isinstance(combined, DataQualityConfig)
    assert len(combined) == 1
    assert combined.rules[0] == simple_rule


def test_add_does_not_mutate_original(
    basic_config: DataQualityConfig, another_rule: ValuesAreUnique
) -> None:
    """__add__ must not modify the original config."""
    original_len = len(basic_config)
    original_rules = list(basic_config.rules)
    _ = basic_config + another_rule
    assert len(basic_config) == original_len
    assert basic_config.rules == original_rules


def test_add_returns_new_object(
    basic_config: DataQualityConfig, another_rule: ValuesAreUnique
) -> None:
    """__add__ must return a distinct object, not the original."""
    combined = basic_config + another_rule
    assert combined is not basic_config


def test_add_unsupported_type_returns_not_implemented(
    basic_config: DataQualityConfig,
) -> None:
    """Adding an unsupported type returns NotImplemented."""
    result = basic_config.__add__("not a rule")  # type: ignore[arg-type]
    assert result is NotImplemented


def test_add_list_with_invalid_item_returns_not_implemented(
    basic_config: DataQualityConfig, simple_rule: ValuesAreComplete
) -> None:
    """Adding a list that contains a non-rule item returns NotImplemented."""
    result = basic_config.__add__([simple_rule, "not a rule"])  # type: ignore[list-item]
    assert result is NotImplemented


# ---- __iadd__ tests ----


def test_iadd_single_rule(
    basic_config: DataQualityConfig, another_rule: ValuesAreUnique
) -> None:
    """'+=' with a single rule mutates the config in place and increments length by 1."""
    original_id = id(basic_config)
    original_len = len(basic_config)
    basic_config += another_rule
    assert id(basic_config) == original_id
    assert len(basic_config) == original_len + 1
    assert basic_config.rules[-1] == another_rule


def test_iadd_list_of_rules(
    basic_config: DataQualityConfig,
    simple_rule: ValuesAreComplete,
    another_rule: ValuesAreUnique,
) -> None:
    """'+=' with a list of rules mutates the config in place."""
    original_len = len(basic_config)
    basic_config += [simple_rule, another_rule]  # type: ignore[operator]
    assert len(basic_config) == original_len + 2


def test_iadd_rule_to_empty_config(simple_rule: ValuesAreComplete) -> None:
    """'+=' on an empty config produces a single-rule config."""
    config = DataQualityConfig()
    config += simple_rule
    assert len(config) == 1
    assert config.rules[0] == simple_rule


def test_iadd_unsupported_type_returns_not_implemented(
    basic_config: DataQualityConfig,
) -> None:
    """'__iadd__' with an unsupported type returns NotImplemented."""
    result = basic_config.__iadd__("not a rule")  # type: ignore[arg-type]
    assert result is NotImplemented


def test_iadd_list_with_invalid_item_returns_not_implemented(
    basic_config: DataQualityConfig, simple_rule: ValuesAreComplete
) -> None:
    """'__iadd__' with a list containing a non-rule item returns NotImplemented."""
    result = basic_config.__iadd__([simple_rule, "not a rule"])  # type: ignore[list-item]
    assert result is NotImplemented
