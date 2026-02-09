# (c) Crown Copyright GCHQ \n
from datetime import datetime

import pytest

from gchq_data_quality.rules.timeliness import (
    TimelinessRelativeRule,
    TimelinessStaticRule,
)
from tests.conftest import (
    assert_dq_result_matches_expected,
    process_test_data_inputs_for_pandas,
)


def test_timeliness_static(timeliness_static_case: dict) -> None:
    """Assumes all values in the 'expected' dictionary in our YAML test data
    correspond directly to the pydantic attributes.

    IF
    expected:
        pass_rate: 1.0
        records_evaluated: 4

    THEN
    We will check that result.pass_rate == 1.0 (expected['pass_rate'])
    """
    inputs, df = process_test_data_inputs_for_pandas(timeliness_static_case)
    result = TimelinessStaticRule(**inputs["inputs"]).evaluate(df)
    assert_dq_result_matches_expected(result, inputs["expected"])


def test_timeliness_relative(timeliness_relative_case: dict) -> None:
    """Assumes all values in the 'expected' dictionary in our YAML test data
    correspond directly to the pydantic attributes.

    IF
    expected:
        pass_rate: 1.0
        records_evaluated: 4

    THEN
    We will check that result.pass_rate == 1.0 (expected['pass_rate'])
    """
    inputs, df = process_test_data_inputs_for_pandas(timeliness_relative_case)
    result = TimelinessRelativeRule(**inputs["inputs"]).evaluate(df)
    assert_dq_result_matches_expected(result, inputs["expected"])


def test_exception_when_start_and_end_date_none() -> None:
    # Both start_date and end_date are None: should raise ValueError due to model validation
    with pytest.raises(
        ValueError, match="At least one of 'start_date' or 'end_date' must be provided"
    ):
        TimelinessStaticRule(field="timestamp", start_date=None, end_date=None)


def test_exception_when_both_reference_date_and_reference_column() -> None:
    with pytest.raises(
        ValueError, match="Provide only reference_date OR reference_column, not both"
    ):
        TimelinessRelativeRule(
            field="timestamp",
            start_timedelta="0d",
            end_timedelta="1d",
            reference_date="2024-06-01T00:00:00Z",
            reference_column="other_column",
        )


def test_warning_if_reference_date_and_reference_column_none() -> None:
    with pytest.warns(UserWarning):
        rule = TimelinessRelativeRule(
            field="timestamp",
            start_timedelta="0d",
            end_timedelta="1d",
            reference_date=None,
            reference_column=None,
        )
        # python validators turn this to datetime.now()
        assert isinstance(rule.reference_date, datetime)


def test_exception_when_start_and_end_timedelta_none() -> None:
    # Both start_timedelta and end_timedelta are None: should raise ValueError due to model validation
    with pytest.raises(
        ValueError,
        match="At least one of 'start_timedelta' or 'end_timedelta' must be provided",
    ):
        TimelinessRelativeRule(
            field="timestamp", start_timedelta=None, end_timedelta=None
        )


def test_invalid_date() -> None:
    # should raise ValueError due to model validation
    with pytest.raises(
        ValueError,
    ):
        TimelinessStaticRule(
            field="timestamp", start_date="bad date", end_date=datetime(2025, 1, 1)
        )
