# (c) Crown Copyright GCHQ \n
from gchq_data_quality.rules.validity import (
    ValidityNumericalRangeRule,
    ValidityRegexRule,
)
from tests.conftest import (
    assert_dq_result_matches_expected,
    process_test_data_inputs_for_pandas,
)


def test_validity_regex(validity_regex_case: dict) -> None:
    """Assumes all values in the 'expected' dictionary in our YAML test data
    correspond directly to the pydantic attributes.

    IF
    expected:
        pass_rate: 1.0
        records_evaluated: 4

    THEN
    We will check that result.pass_rate == 1.0 (expected['pass_rate'])
    """
    inputs, df = process_test_data_inputs_for_pandas(validity_regex_case)
    result = ValidityRegexRule(**inputs["inputs"]).evaluate(df)
    assert_dq_result_matches_expected(result, inputs["expected"])


def test_validity_numerical_range(validity_numerical_range_case: dict) -> None:
    """Assumes all values in the 'expected' dictionary in our YAML test data
    correspond directly to the pydantic attributes.

    IF
    expected:
        pass_rate: 1.0
        records_evaluated: 4

    THEN
    We will check that result.pass_rate == 1.0 (expected['pass_rate'])
    """
    inputs, df = process_test_data_inputs_for_pandas(validity_numerical_range_case)
    result = ValidityNumericalRangeRule(**inputs["inputs"]).evaluate(df)
    assert_dq_result_matches_expected(result, inputs["expected"])
