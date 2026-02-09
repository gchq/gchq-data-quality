# (c) Crown Copyright GCHQ \n
"""
Datetime validation and normalisation utilities for data quality checks.

This data quality package defaults or coerces all datetime values to UTC.
This prevents headaches with British Summer Time and timezones.

Functions provided:
    * Conversion to UTC-aware datetime using vectorised and fallback methods.
    * Vectorised checks for whether datetimes fall between given bounds (absolute or per-row).

Pandas stores datetime information in nanoseconds, and is optimised for this format. However, large date ranges or typos (e.g., year 1066/ 3025)
exceed this range (Out of Bounds error), so sequentially slower but broader methods are attempted. We didn't want to coerce faulty dates like 3025
to NaT and not see them (given the aim of this package is spotting data quality errors)
"""

from datetime import datetime
from warnings import warn

import pandas as pd
from pandas._libs.tslibs.nattype import NaTType


def to_utc_datetime(
    value: str | datetime | pd.Timestamp | float | int | None, dayfirst: bool = False
) -> pd.Timestamp | NaTType:
    """Parse a date or datetime input to a pandas Timestamp.

    Converts various date or datetime representations into a pandas Timestamp. Handles strings, floats, integers,
    and native datetime objects as in pd.to_datetime (integers are considered Unix timestamps - nanosecond since 1 Jan 1970).

    Args:
        val: Input date or time value; accepts str, datetime, pd.Timestamp, float, int or None.
        dayfirst: If True, parses ambiguous date strings where day appears first (e.g. 31/05/2022).

    Returns:
        pd.Timestamp: Pandas-compatible Timestamp in UTC, or pd.NaT if parsing fails.

    Raises:
        None explicitly; function falls back gracefully for out-of-bounds values.

    Examples:
        >>> to_utc_datetime("2024-06-20")
        Timestamp('2024-06-20 00:00:00+00:00', tz='UTC')
        >>> to_utc_datetime(1e40)
        Timestamp(1e+40, tz='UTC')
    """

    if value is None:
        return pd.NaT
    try:
        # Vectorised Pandas parser for scalars
        return pd.to_datetime(value, dayfirst=dayfirst, utc=True, errors="raise")

    except pd.errors.OutOfBoundsDatetime as e_bounds:
        # Fallback for far past/future dates that pandas can't handle in ns precision
        warn(
            f"pd.to_datetime() failed for {value} due to OutOfBoundsDatetime: {e_bounds}. "
            "Attempting pd.Timestamp fallback.",
            stacklevel=2,
        )
        return pd.Timestamp(value, tz="UTC")

    except (ValueError, TypeError) as e:
        # This is the "DateParseError" branch - e.g. "not a date"
        warn(
            f"{value} is not a parseable date, returning NaT. Error: {e}", stacklevel=2
        )
        return pd.NaT


def is_within_datetime_range(
    df: pd.DataFrame,
    field: str,
    start_date: datetime | pd.Timestamp | pd.Series | None = None,
    end_date: datetime | pd.Timestamp | pd.Series | None = None,
) -> pd.Series:
    """Checks whether non-null UTC datetimes in a DataFrame column are within [start_date, end_date] (inclusive).

    The column and bounds should be normalised with to_utc_datetime_series prior to execution.

    Args:
        df: DataFrame containing UTC datetime column.
        field: Name of column in df with UTC datetimes.
        start_date: Minimum datetime boundary (scalar or Series).
        end_date: Maximum datetime boundary (scalar or Series).

    Returns:
        pd.Series: Boolean mask for rows where value is within [start_date, end_date] and is not null.
    """
    data = df[field]
    mask = pd.Series(True, index=data.index)
    if start_date is not None:
        mask = mask & (data >= start_date)
    if end_date is not None:
        mask = mask & (data <= end_date)
    return mask
