# (c) Crown Copyright GCHQ \n
"""Core models and types for data quality package.

Defines:
    - The framework dimension enum (DamaFramework) for standardising data quality rule categories to the DAMA Framework.
    - Common datetime handling utilities and types.
    - Base model with JSON export behaviour for consistent serialisation.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from typing import Annotated

import pandas as pd
from pandas._libs.tslibs.nattype import NaTType
from pydantic import (
    BaseModel,
    BeforeValidator,
    PlainSerializer,
    ValidationInfo,
)

from gchq_data_quality.rules.utils.datetime_utils import to_utc_datetime


class DamaFramework(str, Enum):
    """
    Allowed names for data quality framework dimensions following DAMA (Data Management Association).

    Members:
        Uniqueness: Value is "Uniqueness".
        Completeness: Value is "Completeness".
        Validity: Value is "Validity".
        Consistency: Value is "Consistency".
        Accuracy: Value is "Accuracy".
        Timeliness: Value is "Timeliness".

    Note:
        It will accept any string case, but coerce to title case.

    Example:
        ```python
        DamaFramework("uniqueness")   # Returns DamaFramework.Uniqueness
        DamaFramework.Completeness.value  # "Completeness"
        ```
    """

    Uniqueness = "Uniqueness"
    Completeness = "Completeness"
    Validity = "Validity"
    Consistency = "Consistency"
    Accuracy = "Accuracy"
    Timeliness = "Timeliness"

    @classmethod
    def _missing_(cls, value: object) -> DamaFramework | None:
        """
        Returns the enum member matching the string (case-insensitive title) to help users
        who supply lower-case or upper-case matching names."""
        if isinstance(value, str):
            title_value = value.title()  # allows users to input 'uniqueness'
            for member in cls:
                if member.value == title_value:
                    return member
        return None


DataQualityDimension = Annotated[DamaFramework, PlainSerializer(lambda x: x.value)]

## ------ Functions to help serialize and validate custom pydantic types -----


def _validate_date(
    value: str | datetime | pd.Timestamp | int | float | None, info: ValidationInfo
) -> datetime | None:
    """
    Validates and converts the input value to a UTC datetime for data quality rules.

    Args:
        value: The input, which may be a string, datetime, pandas.Timestamp, int, float, or None.
        info: Pydantic ValidationInfo, used for optional parameters (e.g. dayfirst).

    Returns:
        datetime or None: A UTC datetime instance if conversion succeeded, else None.

    Example:
        ```python
        _validate_date("2024-06-01T10:00:00Z", ValidationInfo(...))
        datetime.datetime(2024, 6, 1, 10, 0, tzinfo=<UTC>)
        ```
    """

    if (value is None) or (pd.isna(value)):
        return None
    else:
        utc_datetime = to_utc_datetime(
            value, info.data.get("dayfirst", False)
        ).to_pydatetime()  # pydantic cannot handle pd.Timestamp

        # invalid dates are turned to NaT, for validating fields this is not wanted.
        if pd.isna(utc_datetime):
            raise ValueError(f"Invalid date value parsed in: {value}")
        else:
            return utc_datetime


def _set_now_if_none(value: datetime | None) -> datetime:
    """The default measurement_time is UTC now if None is provided"""
    if value is None:
        return datetime.now(UTC)
    else:
        return value


def _to_isoformat(value: datetime | pd.Timestamp) -> str:
    """Consistent seriliasation to ISO Format strings for our datetime values"""
    return value.isoformat()


def _to_isoformat_or_none(
    value: datetime | pd.Timestamp | None | NaTType,
) -> str | None:
    """ISO formart strings or None is returned if the value is null - for JSON serialisation"""
    if pd.isnull(value):
        return None
    else:
        return _to_isoformat(value)


# A common time format used across TimelinessRules, ensures we get a valid date
UTCDateTime = Annotated[
    datetime | None,
    BeforeValidator(_validate_date),
    PlainSerializer(_to_isoformat_or_none, when_used="json"),
]

# Strict does not allow value to be None, and defaults to UTC Now if it is
UTCDateTimeStrict = Annotated[
    datetime,
    BeforeValidator(_validate_date),
    BeforeValidator(_set_now_if_none),
    PlainSerializer(_to_isoformat, when_used="json"),
]


class DataQualityBaseModel(BaseModel):
    """
    Base model which applies common behaviours and methods for data quality classes.

    """

    def to_dict(self) -> dict:
        """
        Returns a dictionary serialisation of the model, excluding fields with value None.

        Returns:
            dict: Model data as a dictionary with None values excluded.

        Note:
            equivalent to the pydantic method: model_dump(mode="json", exclude_none=True)
        """
        return self.model_dump(mode="json")

    def to_json(self, path: str | None = None) -> str:
        """
        Serialises the report to a formatted JSON string, optionally saving to disk.

        Args:
            path (str, optional): File path to write the JSON if provided;
                if None, function just returns the string.

        Returns:
            str: JSON representation of the full DataQualityReport.

        Example:
            ```python
            json_str = report.to_json()
            report.to_json('report.json')
            ```
        """

        json_string = self.model_dump_json(indent=2)
        if path:
            with open(path, "w") as f:
                f.write(json_string)
        return json_string
