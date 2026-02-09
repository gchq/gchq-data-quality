# (c) Crown Copyright GCHQ \n
from pathlib import Path
from typing import Any

import pytest
import yaml

from gchq_data_quality.config import (
    DataQualityConfig,
    _load_regex_yaml,
)
from gchq_data_quality.rules.validity import ValidityRegexRule


def test_load_and_replace_patterns(
    test_config_file: Path, test_regex_patterns_file: Path
) -> None:
    """Test that regex values are replaced within the rule config, and that existing regex patterns
    are not overwritten"""

    config_no_replace = DataQualityConfig.from_yaml(test_config_file)

    assert isinstance(config_no_replace.rules[2], ValidityRegexRule)
    assert config_no_replace.rules[2].regex_pattern == "EMAIL_REGEX"

    assert isinstance(config_no_replace.rules[3], ValidityRegexRule)
    assert (
        config_no_replace.rules[3].regex_pattern == r"^[a-zA-Z\s]+$"
    ), "Name regex should remain unchanged"

    # No replace regex with YAML regex manager
    config = DataQualityConfig.from_yaml(
        test_config_file, regex_yaml_path=test_regex_patterns_file
    )

    assert isinstance(config.rules[2], ValidityRegexRule)
    rule_2_regex = config.rules[2].regex_pattern

    assert isinstance(config.rules[3], ValidityRegexRule)
    rule_3_regex = config.rules[3].regex_pattern

    assert (
        rule_2_regex
        and rule_2_regex == r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    ), "This should NOT be EMAIL_REGEX (has been replaced)"
    assert (
        rule_3_regex and rule_3_regex == r"^[a-zA-Z\s]+$"
    ), "Name regex should remain unchanged"


def test_load_regex_patterns_yaml(test_regex_patterns_file: Path) -> None:
    patterns = _load_regex_yaml(test_regex_patterns_file)
    # Check that the expected keys exist
    assert "EMAIL_REGEX" in patterns
    assert "PHONE_REGEX" in patterns
    assert patterns["EMAIL_REGEX"].startswith("^")
    assert patterns["PHONE_REGEX"].endswith("$")


def test_load_data_quality_config_files_single(test_config_file: Path) -> None:
    result = DataQualityConfig.from_yaml(test_config_file)

    assert isinstance(result, DataQualityConfig)
    assert result.dataset_name == "dataset"
    assert len(result.rules) == 4


TEST_DIR = Path(__file__).parents[1] / "artifacts" / "test_directory"
assert TEST_DIR.exists()
FILE_01 = TEST_DIR / "data_quality_rules_01.yaml"
FILE_02 = TEST_DIR / "data_quality_rules_02.yaml"


def test_load_multiple_files_merges_rules_and_warns() -> None:
    files = [FILE_01, FILE_02]
    with pytest.warns(UserWarning, match="Multiple configuration files loaded"):
        config = DataQualityConfig.from_yaml(files)
    assert isinstance(config, DataQualityConfig)
    # we should get top level values from the 1st file, but all of the rules (2 per file * 2 = 4)
    assert config.dataset_name == "data_quality_rules_01"
    assert config.measurement_sample == "1st file"
    assert len(config.rules) == 4
    rule_ids = [r.rule_id for r in config.rules]
    assert set(rule_ids) == set(["rule01", "rule02", "rule03", "rule04"])


def test_load_data_quality_config_files_file_not_found() -> None:
    missing_file = TEST_DIR / "DOES_NOT_EXIST.yaml"
    with pytest.raises(FileNotFoundError, match="Config file not found"):
        DataQualityConfig.from_yaml(missing_file)


# Regex loading and errors
def _write_yaml(file_path: str | Path, obj: Any) -> None:  # noqa ANN401
    with open(file_path, "w") as f:
        yaml.dump(obj, f)


def test_load_regex_yaml_not_dict(tmp_path: Path) -> None:
    # YAML content: just a list, not a dict
    path = tmp_path / "bad_regex_not_dict.yaml"
    _write_yaml(path, ["foo", "bar"])
    with pytest.raises(ValueError, match="Regex YAML file should contain a dictionary"):
        _load_regex_yaml(path)


def test_load_regex_yaml_value_not_string(tmp_path: Path) -> None:
    # Dict, but one value is not a string
    path = tmp_path / "bad_regex_value_not_string.yaml"
    _write_yaml(path, {"EMAIL_REGEX": 123, "FOO": "bar"})
    with pytest.raises(ValueError, match="is not a string"):
        _load_regex_yaml(path)
