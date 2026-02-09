# (c) Crown Copyright GCHQ \n
"""We dynamically generate test cases from YAML files in the tests/data directory

a file called completeness.yaml will resolve to a series of test cases within a
pytest fixture called completeness_case

Two top level keys are used when handling these cases.
1. inputs (key-value pairs for function input) **case['inputs']
2. description - used by pytest as the id of the test
3. expected - the expected value of the test (dictionary of key-value pairs)"""

import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest
import yaml

from gchq_data_quality.config import DataQualityConfig
from gchq_data_quality.models import DamaFramework
from gchq_data_quality.results.models import DataQualityReport, DataQualityResult

DATA_DIR = Path(__file__).parent / "data"

# so we can test for exceptions in our test files by passing in strings
EXCEPTION_MAP = {
    "ValueError": ValueError,
    "KeyError": KeyError,
    "TypeError": TypeError,
}


@pytest.fixture
def basic_data_quality_result() -> DataQualityResult:
    minimal_result = DataQualityResult(
        dataset_name="test_dataset",
        dataset_id=1,
        measurement_sample=None,
        measurement_time=datetime(2025, 1, 1),
        lifecycle_stage="test",
        data_quality_dimension=DamaFramework.Completeness,
        field="test_field",
        rule_id="R1",
        rule_description="Dummy rule",
        rule_data=json.dumps({"field": "test_field", "function": "completeness"}),
        records_evaluated=1,
        records_failed_ids=[1, 2, 3, 4, 5],
        records_failed_sample=[{"id": 1}],
        pass_rate=1.0,
    )
    return minimal_result


@pytest.fixture
def basic_data_quality_report(
    basic_data_quality_result: DataQualityResult,
) -> DataQualityReport:
    # Define a minimal DataQualityResult dummy instance.

    report = DataQualityReport(results=[basic_data_quality_result])
    return report


def process_test_data_inputs_for_pandas(input_dict: dict) -> tuple[dict, pd.DataFrame]:
    """Handle input test cases for pandas evaluation, where we pull out any dataframe
    to pass into the evaluate function"""

    df_data = input_dict["inputs"].pop("df", None)

    if df_data is None:
        df = pd.DataFrame()
    else:
        df = pd.DataFrame.from_records(df_data)

    return input_dict, df


def get_kwargs_for_dq_config(input_dict: dict) -> dict:
    """Pulls out kwargs that are valid entries into DataQualityConfig (which forbids non-valid kwargs),
    Allows us to pass any matching attribute from our test data YAML files"""

    return {k: v for k, v in input_dict.items() if k in DataQualityConfig.model_fields}


def try_parse_records_failed_sample(
    val: str | datetime | None,
) -> datetime | None | str:
    """Spark returns datetime failed recordes as str, but our unit tests validate against a datetime"""
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val)
        except ValueError:
            return val
    return val


def assert_dq_result_matches_expected(
    result: DataQualityResult, expected: dict, ignore_records_failed_ids: bool = False
) -> None:
    """
    Compare attributes from a Pydantic DQ Entry to 'expected' values after JSON serialisation.
    Only keys in 'expected' are compared.

    Useful for testing the base functionality of each DQ functions - giving expected values and number of
    results

    In Spark we never return records_failed_ids due to it being unordered, so we need to exclude that from
    some tests

    If we need to verify actual types and non-JSON values this will be done separately
    """
    model_json_dict = result.model_dump(mode="python")  # JSON-friendly formats

    if ignore_records_failed_ids:
        expected.pop("records_failed_ids", None)

    for key, exp_val in expected.items():
        assert key in model_json_dict, f"Model missing expected attribute '{key}'"

        result_value = model_json_dict[key]

        # when processing data through Spark we can't guarantee the order of our records_failed_sample values
        if (key == "records_failed_sample") and (exp_val is not None):
            # get sets of values in each record (order agnostic) - we need to allow for the fact that when in Spark
            # we get values after JSON serialisation, so things like datetimes will be strings
            exp_set = {
                frozenset((k, try_parse_records_failed_sample(v)) for k, v in d.items())
                for d in exp_val
            }
            res_set = {
                frozenset((k, try_parse_records_failed_sample(v)) for k, v in d.items())
                for d in result_value
            }

            assert res_set == exp_set

        # Spark may introduce floating point value rounding inaccuracies at high numbers of d.p.
        elif isinstance(result_value, float) and isinstance(exp_val, float):
            assert result_value == pytest.approx(exp_val, 1e-4)
        else:
            assert result_value == exp_val


def load_yaml_test_data(yaml_path: Path) -> list:
    """Load and validate a YAML test data file.

    Converts {col_name : [1,2,3,4]} dictionaries to dataframes assuming they have a 'df' key
    Checks some high level formatting in the YAML file
    """
    with open(yaml_path) as f:
        cases = yaml.safe_load(f)

    if not isinstance(cases, list):
        raise ValueError(f"{yaml_path.name} must contain a list of test cases.")

    validated_cases = []
    for i, case in enumerate(cases, start=1):
        # Check required keys
        for key in ("description", "inputs", "expected"):
            if key not in case:
                raise ValueError(f"Missing '{key}' in case #{i} of {yaml_path.name}")
        # Check types
        if not isinstance(case["inputs"], dict):
            raise ValueError(
                f"'inputs' must be a dict in case #{i} of {yaml_path.name}"
            )
        if not isinstance(case["expected"], dict | list):
            raise ValueError(
                f"'expected' must be a dict or list in case #{i} of {yaml_path.name}"
            )

        # Only auto-convert an input key exactly called 'df' to a DataFrame
        if "df" in case["inputs"]:
            df_data = case["inputs"]["df"]
            if isinstance(df_data, dict):  # column name → values
                case["inputs"]["df"] = pd.DataFrame(df_data)
            else:
                raise ValueError(
                    f"'df' in case #{i} of {yaml_path.name} must be a dict of columns → values."
                )

        validated_cases.append(case)

    return validated_cases


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """
    Auto-parametrise tests from YAML files in /tests/data.
    A YAML file named e.g. `uniqueness.yml` creates a fixture `uniqueness_case`.

    The YAML file contains:
    1. description - an explanation of the unit test that is printed at runtime by pytest
    2. inputs - the parameters to the function as key-value pairs that get expanded **case['inputs'] in the function call
    3. expected - the expected outputs which we map one-to-one in each unit test
    """
    for yaml_file in DATA_DIR.glob("*.yaml"):
        fixture_name = yaml_file.stem + "_case"  # uniqueness.yml -> uniqueness_case
        if fixture_name in metafunc.fixturenames:
            cases = load_yaml_test_data(yaml_file)
            metafunc.parametrize(
                fixture_name, cases, ids=[case["description"] for case in cases]
            )


@pytest.fixture
def test_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 3, 5],  # 4 /5 unique
            "name": ["John", "Jane", "Dave", None, "Alice"],  # 1 null value
            "age": [30, 25, 102, 15, -5],  # a negative age
            "email": [
                "john@example.com",
                "jane@example.com",
                "dave@example",
                "test@test.com",
                "alice@example.com",
            ],  # invalid email
            "category": ["A", "B", "C", "D", "E"],
            "score": [10, 20, 30, 40, 50],
            "date": [
                datetime(2023, 1, 1),
                datetime(2023, 2, 1),
                datetime(2023, 3, 1),
                datetime(2021, 1, 1),  # one date too old
                datetime(2023, 5, 1),
            ],
        }
    )


@pytest.fixture
def test_config_file() -> Path:
    test_yaml_path = Path(__file__).parent / "artifacts" / "test_config.yaml"
    return test_yaml_path


@pytest.fixture
def test_regex_patterns_file() -> Path:
    test_yaml_path = Path(__file__).parent / "artifacts" / "test_patterns.yaml"
    return test_yaml_path
