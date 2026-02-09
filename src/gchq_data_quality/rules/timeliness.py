# (c) Crown Copyright GCHQ \n
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Literal, Self
from warnings import warn

import pandas as pd
from pydantic import Field, model_validator

from gchq_data_quality.models import DamaFramework, DataQualityDimension, UTCDateTime
from gchq_data_quality.rules.base import BaseRule
from gchq_data_quality.rules.utils.datetime_utils import (
    is_within_datetime_range,
    to_utc_datetime,
)


class TimelinessBaseRule(BaseRule):
    """Coercian to UTC datetime is common functionality between our timeliness functions."""

    dayfirst: bool = Field(
        default=False,
        description="Passed into pd.to_datetime during date handling if the date is day or month first. Only apply this if your format is DD/MM/YYYY or similar.",
    )
    data_quality_dimension: DataQualityDimension = Field(
        default=DamaFramework.Timeliness
    )

    def _coerce_dataframe_type(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.apply(self._to_utc_datetime_series)

    def _to_utc_datetime_series(self, series: pd.Series) -> pd.Series:
        """Converts a pandas Series to UTC-aware Timestamps; handles out-of-bounds dates gracefully."""
        try:
            # Try fast vectorised conversion with ns precision
            return pd.to_datetime(
                series, utc=True, dayfirst=self.dayfirst, errors="raise"
            )
        except ValueError:
            # Fallback: individual parsing (slower but handles far-future dates)
            return series.map(lambda x: to_utc_datetime(x, dayfirst=self.dayfirst))


class TimelinessStaticRule(TimelinessBaseRule):
    """
    Rule to check whether datetime values in a column fall between absolute start and end date boundaries (inclusive).

    Suitable where both boundaries are fixed or known in advance (e.g., events occurring in January 2024).
    All dates are treated as, or coerced to, UTC, with date-only strings assumed to be midnight. Invalid or unparsable datetime
    values are treated as missing. Combine with a validity rule and completeness rule on the same field for the best insights.

    Attributes:
        field (str): Name of the datetime column to assess.
        start_date (str | datetime | pd.Timestamp | None): Inclusive lower boundary for valid values.
        end_date (str | datetime | pd.Timestamp | None): Inclusive upper boundary for valid values.
        dayfirst (bool): If True, parses ALL dates and rule inputs as day/month/year, otherwise month/day/year.
        na_values (str | list[Any] | None): Values treated as missing.
        data_quality_dimension (DamaFramework): Associated data quality dimension.
            (You may want to override, e.g. perhaps 'Consistency' makes sense for some of these rules)
        rule_id (str | None): Optional rule identifier.
        rule_description (str | None): Optional rule description.

    Methods:
        evaluate(data_source: pd.DataFrame | SparkDataFrame) -> DataQualityResult
            Evaluates the rule on the provided Pandas or Spark DataFrame and returns
            the metrics and diagnostics of the rule evaluation.

    Example:
        ```python
        >>> rule = TimelinessStaticRule(
        ...     field="event_date",
        ...     start_date="2024-01-01T00:00:00Z",
        ...     end_date="2024-01-31T23:59:59Z"
        ... )
        >>> result = rule.evaluate(df)

        # Only require that dates are on or after 2023-06-01
        >>> rule = TimelinessStaticRule(
        ...     field="date_col",
        ...     start_date="2023-06-01",
        ...     end_date=None
        ... )
        >>> result = rule.evaluate(df)

        # Using string-based boundaries with day-first format
        >>> rule = TimelinessStaticRule(
        ...     field="timestamp",
        ...     start_date="01/06/2023",
        ...     end_date="30/06/2023",
        ...     dayfirst=True # also assumes dates in field 'timestamp' are dayfirst
        ... )
        >>> result = rule.evaluate(df)

        # Using Python datetime objects as boundaries
        >>> from datetime import datetime, timezone
        >>> rule = TimelinessStaticRule(
        ...     field="timestamp",
        ...     start_date=datetime(2023, 6, 1, 0, 0, tzinfo=timezone.utc),
        ...     end_date=datetime(2023, 6, 30, 23, 59, tzinfo=timezone.utc),
        ... )
        >>> result = rule.evaluate(df)
        ```

    Returns:
        DataQualityResult: Contains the timeliness score (`pass_rate`), indices of failed records,
        a sample of those records, and metadata. See DataQualityResult documentation for details.
    """

    function: Literal["timeliness_static"] = "timeliness_static"

    start_date: UTCDateTime = Field(
        default=None, description="Earliest allowed timestamp (inclusive)"
    )
    end_date: UTCDateTime = Field(
        default=None,
        description="Latest allowed timestamp (exclusive, leave blank for open-ended)",
    )

    @model_validator(mode="after")
    def at_least_one_date(self) -> TimelinessStaticRule:
        if self.start_date is None and self.end_date is None:
            raise ValueError(
                f"At least one of 'start_date' or 'end_date' must be provided. {self.start_date=}, {self.end_date=}"
            )
        return self

    def _get_records_passing_mask_pandas(self, df: pd.DataFrame) -> pd.Series:
        """A records passes if it is within the datetime boundary"""
        records_passing_mask = is_within_datetime_range(
            df, self.field, self.start_date, self.end_date
        )
        return records_passing_mask


class TimelinessRelativeRule(TimelinessBaseRule):
    """
    Rule to assess whether datetime values fall between relative time boundaries
    from a reference date (which can be a static value or come from a column in the data source).

    Timedelta bounds are specified for start and end, relative to a reference date or reference column.
    All datetime comparisons are performed in UTC, with date-only values assumed midnight.
    Only one of `reference_date` or `reference_column` may be provided. If neither is given,
    current UTC time is used as the reference_date.

    Attributes:
        field (str): Name of the datetime column to assess.
        start_timedelta (timedelta | str | int | float | None): Lower offset from the reference.
        end_timedelta (timedelta | str | int | float | None): Upper offset from the reference.
        reference_date (str | datetime | pd.Timestamp | None): Fixed reference date/time (UTC).
        reference_column (str | None): Per-row column providing reference dates/times.
        dayfirst (bool): If True, parses ALL dates as day/month/year.
        na_values (str | list[Any] | None): Values treated as missing.
        data_quality_dimension (DamaFramework): Associated data quality dimension.
        rule_id (str | None): Optional rule identifier.
        rule_description (str | None): Optional rule description.

    Methods:
        evaluate(data_source: pd.DataFrame | SparkDataFrame) -> DataQualityResult
            Evaluates the rule on the provided Pandas or Spark DataFrame and returns
            the metrics and diagnostics of the rule evaluation.

    Note:
        Integer values into start or end timedelta are assumed to be nanoseconds (default pandas.to_timedelta() behaviour)

    Example:
        ```python
        >>> rule = TimelinessRelativeRule(
        ...     field="event_date",
        ...     start_timedelta="0d",
        ...     end_timedelta="30d",
        ...     reference_date="2024-01-01T00:00:00Z"
        ... )
        >>> result = rule.evaluate(df)

        >>> rule = TimelinessRelativeRule(
        ...     field="booking_date",
        ...     start_timedelta="-1d",
        ...     end_timedelta="5d",
        ...     reference_column="event_date"
        ... )
        >>> result = rule.evaluate(df)

        # Require event dates at least 5 days after the reference date
        >>> rule = TimelinessRelativeRule(
        ...     field="event_date",
        ...     start_timedelta="5d",
        ...     end_timedelta=None,
        ...     reference_date="2023-06-01"
        ... )
        >>> result = rule.evaluate(df)

        >>> from datetime import timedelta
        >>> rule = TimelinessRelativeRule(
        ...     field="sensor_timestamp",
        ...     start_timedelta=timedelta(hours=-12),
        ...     end_timedelta=timedelta(hours=12)
        ... )
        >>> result = rule.evaluate(df)
        ```

    Returns:
        DataQualityResult: Contains the timeliness score (`pass_rate`), indices and sample of failed records,
        total records evaluated, and metadata. See DataQualityResult documentation for details.
    """

    function: Literal["timeliness_relative"] = "timeliness_relative"
    start_timedelta: timedelta | None = Field(
        default=None,
        description="Minimum allowed timedelta difference (e.g., -2 days). Accepts timedelta, string or numeric (nanoseconds).",
    )
    end_timedelta: timedelta | None = Field(
        default=None,
        description="Maximum allowed timedelta difference (e.g., 0 days). Accepts timedelta, string or numeric (nanoseconds).",
    )
    reference_date: UTCDateTime | None = Field(
        default=None,
        description="Fixed reference date to compare field's datetime with (UTC). Use either this or reference_column. dayfirst impacts how this value is interpreted",
    )
    reference_column: str | None = Field(
        default=None,
        description="Reference column for row-wise comparison. Mutually exclusive with reference_date.",
    )

    @model_validator(mode="after")
    def _check_only_one_reference_date_field(self) -> TimelinessRelativeRule:
        if self.reference_date is not None and self.reference_column is not None:
            raise ValueError(
                f"Provide only reference_date OR reference_column, not both. reference_date={self.reference_date}, reference_column={self.reference_column}"
            )

        if self.reference_date is None and self.reference_column is None:
            self.reference_date = datetime.now(UTC)
            warn(
                f"No reference_date or reference_column given; defaulting to a reference_date of 'now':({self.reference_date}).",
                stacklevel=2,
            )

        return self

    @model_validator(mode="after")
    def at_least_one_timedelta(self) -> TimelinessRelativeRule:
        if self.start_timedelta is None and self.end_timedelta is None:
            raise ValueError(
                f"At least one of 'start_timedelta' or 'end_timedelta' must be provided. {self.start_timedelta=}, {self.end_timedelta=}"
            )
        return self

    def _get_columns_used_pandas(self) -> list:
        """A list of columns used in the rule - we include the reference_column if it is set"""
        columns_used = [self.field]
        if self.reference_column:
            columns_used.append(self.reference_column)
        return columns_used

    def _get_reference_date_series(self, df: pd.DataFrame) -> pd.Series:
        """Get a pandas UTC Series of the reference dates for each record,
        either a series of reference column or a repetition of the reference_date"""
        if self.reference_column:
            reference_dates = df[self.reference_column]
        else:
            reference_dates = pd.Series(self.reference_date, index=df.index)

        utc_reference_dates = self._to_utc_datetime_series(reference_dates)
        return utc_reference_dates

    def _get_date_boundary(
        self, reference_dates: pd.Series, time_delta: timedelta | None
    ) -> pd.Series | None:
        """Generating the start and end time boundary based on the reference date
        and timedelta"""

        if time_delta is None:
            return None
        else:
            return reference_dates + time_delta

    def _get_records_passing_mask_pandas(self, df: pd.DataFrame) -> pd.Series:
        """Records pass if the date in df[field] is within the upper and lower boundary
        defined by the start and end timedelta relative to the reference date."""
        reference_dates = self._get_reference_date_series(df)

        valid_start_dates = self._get_date_boundary(
            reference_dates, self.start_timedelta
        )
        valid_end_dates = self._get_date_boundary(reference_dates, self.end_timedelta)
        records_passing_mask = is_within_datetime_range(
            df, self.field, valid_start_dates, valid_end_dates
        )
        return records_passing_mask

    def _get_spark_safe_rule(self) -> Self:
        """We override the default behaviour as we additionally need to make
        the reference_column spark_safe"""
        from gchq_data_quality.spark.utils.rules_utils import (
            get_spark_safe_column_name,
        )

        rule_copy = self.model_copy()
        rule_copy.field = get_spark_safe_column_name(self.field)

        if self.reference_column:
            rule_copy.reference_column = get_spark_safe_column_name(
                self.reference_column
            )

        return rule_copy
