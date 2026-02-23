# (c) Crown Copyright GCHQ \n
import pytest

from gchq_data_quality.rules.consistency import ConsistencyRule
from tests.conftest import (
    assert_dq_result_matches_expected,
    process_test_data_inputs_for_pandas,
)


def test_consistency(consistency_case: dict) -> None:
    """Assumes all values in the 'expected' dictionary in our YAML test data
    correspond directly to the pydantic attributes.

    IF
    expected:
        pass_rate: 1.0
        records_evaluated: 4

    THEN
    We will check that result.pass_rate == 1.0 (expected['pass_rate'])
    """

    inputs, df = process_test_data_inputs_for_pandas(consistency_case)
    result = ConsistencyRule(**inputs["inputs"]).evaluate(df)
    assert_dq_result_matches_expected(result, inputs["expected"])


def test_consistency_dict_missing_keys() -> None:
    """Expression dict missing 'if' or 'then' should raise ValueError."""
    with pytest.raises(
        ValueError, match="Expression dict must contain both 'if' and 'then' keys"
    ):
        ConsistencyRule(field="foo", expression={"if": "`bar` == 1"})

    with pytest.raises(
        ValueError, match="Expression dict must contain both 'if' and 'then' keys"
    ):
        ConsistencyRule(field="foo", expression={"then": "`bar` == 1"})

    with pytest.raises(
        ValueError, match="Expression dict must contain both 'if' and 'then' keys"
    ):
        ConsistencyRule(field="foo", expression={"something": "`bar` == 1"})
