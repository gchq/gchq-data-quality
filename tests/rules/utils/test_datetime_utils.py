# (c) Crown Copyright GCHQ \n
import pandas as pd

from gchq_data_quality.rules.utils.datetime_utils import (
    is_within_datetime_range,
    to_utc_datetime,
)


def test_to_utc_datetime(to_utc_datetime_case: dict) -> None:
    """See to_utc_datetime.yaml test cases"""

    result_date = to_utc_datetime(**to_utc_datetime_case["inputs"])
    expected_date = to_utc_datetime_case["expected"]["utc_datetime"]
    if expected_date is None:
        assert pd.isna(expected_date)
    else:
        assert result_date == pd.Timestamp(expected_date)


def test_is_within_datetime_range(is_within_datetime_range_case: dict) -> None:
    """See is_within_datetime_range.yaml test cases"""
    result_mask = is_within_datetime_range(**is_within_datetime_range_case["inputs"])
    expected_mask = is_within_datetime_range_case["expected"]["mask"]
    assert result_mask.tolist() == expected_mask


def test_to_utc_datetime_invalid_string() -> None:
    """Should return pd.NaT for an invalid date string"""
    result = to_utc_datetime("not_a_date")
    assert pd.isna(result)


def test_to_utc_datetime_invalid_type() -> None:
    """Should return pd.NaT for totally invalid type (e.g., dict)"""
    result = to_utc_datetime({"year": 2024})
    assert pd.isna(result)


def test_to_utc_datetime_none_returns_nat() -> None:
    """Should return pd.NaT for None input"""
    result = to_utc_datetime(None)
    assert pd.isna(result)
