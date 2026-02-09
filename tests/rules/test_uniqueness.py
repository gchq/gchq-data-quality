# (c) Crown Copyright GCHQ \n
from gchq_data_quality.rules.uniqueness import UniquenessRule
from tests.conftest import (
    assert_dq_result_matches_expected,
    process_test_data_inputs_for_pandas,
)


def test_uniqueness(uniqueness_case: dict) -> None:
    """Assumes all values in the 'expected' dictionary in our YAML test data
    correspond directly to the pydantic attributes.

    expected:
        pass_rate: 1.0
        records_evaluated: 4

    We will check that result.pass_rate == 1.0 (expected['pass_rate'])
    """

    inputs, df = process_test_data_inputs_for_pandas(uniqueness_case)
    result = UniquenessRule(**inputs["inputs"]).evaluate(df)
    assert_dq_result_matches_expected(result, inputs["expected"])
