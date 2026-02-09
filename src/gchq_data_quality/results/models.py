# (c) Crown Copyright GCHQ \n
"""
Defines models representing the results of data quality rules, including
individual rule evaluations and aggregate reports.

Major classes:
    - DataQualityResult: Captures the outcome of a single rule on a field.
    - DataQualityReport: Aggregates multiple DataQualityResults into a user-friendly report.

Typical usage:
    config = DataQualityConfig.from_yaml('my_config.yaml') # see config file
    report = config.execute(df)  # Returns DataQualityReport
    df_results = report.to_dataframe()
    json_results = report.to_json()
"""

from __future__ import annotations

import json
import warnings
from datetime import UTC, datetime
from typing import Any

import numpy as np
import pandas as pd
from pydantic import (
    Field,
    FieldSerializationInfo,
    ValidationInfo,
    field_serializer,
    field_validator,
)

from gchq_data_quality.globals import SampleConfig
from gchq_data_quality.models import (
    DataQualityBaseModel,
    DataQualityDimension,
    UTCDateTimeStrict,
)
from gchq_data_quality.results.utils import (
    add_records_passing,
    aggregate_records_failed_ids,
    aggregate_records_failed_samples,
    coerce_nan_to_none,
    shift_records_failed_ids,
)
from gchq_data_quality.rules.utils.rules_utils import (
    calculate_pass_rate,
)


class DataQualityResult(DataQualityBaseModel):
    """
    Represents the outcome of a single data quality rule applied to a dataset column.
    Noting that some rules may reference additional columns, such as ConsistencyRule

    Attributes:
        dataset_name (float | str | int | None): Common, human-readable name of the measured dataset.
        dataset_id (float | str | int | None): Machine-readable unique ID for the dataset.
        measurement_sample (str | None): Description of the sample measured.
        lifecycle_stage (Any | None): Stage of data lifecycle at the time of measurement (e.g., '01 ingest').
        measurement_time (UTCDateTimeStrict): UTC timestamp when measurement was taken. Defaults to 'now' in UTC.
        field (str): Name of the column the rule applies to.
        data_quality_dimension (DamaFramework): Data quality dimension evaluated (Uniqueness, Completeness, etc.).
        records_evaluated (int | None): Total records evaluated by this rule.
        pass_rate (float | None): Ratio (0-1) of passing records to evaluated records.
        rule_id (Any | None): Local identifier for the applied rule.
        rule_description (Any): Text, dict, or JSON describing rule parameters and logic.
        rule_data (str): JSON dump of rule metadata for reconstruction of rule.
        records_failed_ids (list | None): Up to 10 (default) identifiers for rows failing the rule.
        records_failed_sample (list[dict] | None): Sample output of failed records for diagnostics.

    Example:
        ```python
        # Typical user interaction is via DataQualityReport:
        config = DataQualityConfig.from_yaml('config.yaml')
        report = config.execute(df)
        first_result = report.results[0]
        print(first_result.pass_rate)  # Access result attributes
        ```

    Note:
        Direct construction of DataQualityResult or DataQualityReport are rare; results are typically gathered in production using
        RuleType.evaluate(df) or DataQualityConfig.execute(data)
    """

    dataset_name: float | str | int | None = Field(
        default=None,
        description="Common name (human readable) of the dataset being measured. Use dataset_id if it links to a data catalogue",
    )
    dataset_id: float | str | int | None = Field(
        default=None,
        description="Machine readable ID (useful for integration with catalogue systems etc).",
    )
    measurement_sample: str | None = Field(
        default=None,
        description="Description of the data sample measured. Use 'all' for full dataset.",
    )
    lifecycle_stage: Any | None = Field(
        default=None,
        description="Stage in data lifecycle at time of measurement, e.g., '01 ingest'.",
    )
    measurement_time: UTCDateTimeStrict = Field(
        description="UTC timestamp at which the measurement was captured. Defaults to UTC now if not set or if None",
        default_factory=lambda: datetime.now(UTC),
    )

    field: str = Field(
        ..., description="Column name in the dataset that the rule applies to."
    )
    data_quality_dimension: DataQualityDimension = Field(
        ...,
        description="Which DAMA quality framework area is evaluated (Uniqueness, Completeness, etc.).",
    )
    records_evaluated: int | None = Field(
        default=None,
        description="Total number of records evaluated / checked for this rule.",
    )
    pass_rate: float | None = Field(
        ...,
        ge=0,
        le=1,
        description="Ratio of passing records to records evaluated: between 0 and 1 (inclusive). If not applicable (e.g. no evaluated records) it is None",
    )
    rule_id: Any | None = Field(
        default=None, description="Local identifier for the applied rule."
    )
    rule_description: Any = Field(
        default=None,
        description="Text, dict, or JSON string capturing rule logic or parameters used. If left blank, "
        "all rule metadata will be JSON dumped into the field, allowing the rule to be reconstructed with "
        "config = DataQualityConfig.from_report(my_data_quality_report)",
    )
    rule_data: str = Field(
        description="A JSON dump of the rule information, such that it can be recreated"
    )
    records_failed_ids: list | None = Field(
        default=None,
        description="List of up to 10 row numbers (or indexes) failing the rule.",
    )
    records_failed_sample: list[dict] | None = Field(
        default=None,
        description="Representative failed output sample for diagnostics (e.g. failed values).",
    )

    @field_serializer("records_failed_sample", when_used="json")
    def _serialize_records_failed_sample(self, value: list[dict] | None) -> list | None:
        """We need to make sure that values like NaT and np.NaN are turned to None, as these
        do not JSON Serialize"""
        if value is None:
            return None
        return coerce_nan_to_none(value)

    @field_serializer("records_failed_ids", when_used="always")
    def _serialize_records_failed_ids(
        self, value: list | None, info: FieldSerializationInfo
    ) -> None | list:
        if value is None:
            return None

        # Apply shift if provided in dump context (e.g. in to_dataframe())
        shift = info.context.get("records_failed_ids_shift", 0) if info.context else 0
        value = shift_records_failed_ids(value, shift)

        return value

    def _to_spark_schema_df(self) -> pd.DataFrame:
        """Outputs a dataframe that is compatible with the Spark DataQualityResult Schema.
        Pyarrow has limitations on what it can process, so we need to serialise the records_failed_samples
        and records_failed_ids into a string.

        Returns:
            pd.DataFrame: Spark schema comptabile dataframe
        """

        result_dict = self.to_dict()
        result_dict["records_failed_sample"] = json.dumps(
            result_dict["records_failed_sample"]
        )
        result_dict["records_failed_ids"] = json.dumps(
            result_dict["records_failed_ids"]
        )

        return pd.DataFrame([result_dict])

    @field_validator("records_failed_sample", "records_failed_ids", mode="before")
    @classmethod
    def _parse_records_failed_sample(
        cls, v: str | None | list[dict], info: ValidationInfo
    ) -> list[dict] | None:
        """If it's a string we need to load back into a list[dict] - this happens during
        Spark execution"""
        if v is None:
            return None
        elif isinstance(v, list):  # already deserialized
            return v
        else:
            try:
                return json.loads(v)
            except json.JSONDecodeError:
                warnings.warn(
                    f"Failed to decode field '{info.field_name}' as JSON. Returning None.",
                    stacklevel=2,
                )
                return None

    @field_validator("pass_rate", mode="before")
    @classmethod
    def _set_to_none_if_nan(cls, v: float | None) -> float | None:
        """Needed as Spark coerces to NaN"""
        if pd.isna(v):
            return None
        else:
            return v

    def _set_records_failed_sample(self, records_failed: list[dict] | None) -> None:
        """Limit the number of failing records captured"""
        if records_failed is None:
            self.records_failed_sample = None
        else:
            limit = SampleConfig.RECORDS_FAILED_SAMPLE_SIZE
            self.records_failed_sample = records_failed[:limit]


class DataQualityReport(DataQualityBaseModel):
    """
    A collection of individual data quality results for a dataset.
    This object is typically returned by executing a DataQualityConfig object, rather than
    instantiated directly by the user.

    Attributes:
        results (list[DataQualityResult]): List of individual DataQualityResults for each rule applied.

    Methods:
        to_dataframe(decimals=4, measurement_time_format=None, records_failed_ids_shift=0):
            Converts report results to a pandas DataFrame for analysis.
        to_json(path=None):
            Serialises the report to JSON, optionally saving to file.
        from_dataframe(df):
            Constructs a DataQualityReport from a pandas DataFrame formatted as in to_dataframe().

    Example:
        ```python
        config = DataQualityConfig.from_yaml('quality_cfg.yaml')
        report = config.execute(df) # <- this is the DataQualityReport object creation step
        df_results = report.to_dataframe(decimals=3)
        report.to_json('results.json')
        ```

    """

    results: list[DataQualityResult] = Field(default_factory=list)

    def to_dataframe(
        self,
        decimals: int = 4,
        measurement_time_format: str | None = None,
        records_failed_ids_shift: int = 0,
    ) -> pd.DataFrame:
        """
        Converts the report into a pandas DataFrame for analysis, with optional formatting.

        Args:
            decimals (int): Number of decimal places to round pass_rate values (default: 4).
            measurement_time_format (str | None): If provided, formats 'measurement_time' using Python strftime directives (e.g. '%Y-%m-%d %H:%M:%S').
                If None, leaves the datetime unformatted.
            records_failed_ids_shift (int): If shifting row numbers (e.g. aligning with Excel, set it to 2), adds this value to each failed row number.

        Returns:
            pd.DataFrame: DataFrame with the report data, formatted for readability.

        Example:
            ```python
            df = report.to_dataframe(decimals=2, measurement_time_format="%Y-%m-%d")
            ```
        """

        df = pd.DataFrame(
            [
                record.model_dump(
                    mode="python",
                    exclude_none=False,
                    context={"records_failed_ids_shift": records_failed_ids_shift},
                )
                for record in self.results
            ]
        )

        if not df.empty:
            df = self._round_pass_rate(df, decimals=decimals)

            if measurement_time_format:
                df = _format_measurement_time(df, measurement_time_format)

            df.replace(np.nan, None, inplace=True)
            return df
        else:
            return pd.DataFrame()

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame) -> DataQualityReport:
        """
        Creates a DataQualityReport from a pandas DataFrame, reconstructing
        DataQualityResult objects from each row. Typically not instantiated by the user, but used
        internally to enforce validation and correct handling of NULLS during use within Spark.

        Args:
            df: DataFrame structured similarly to the output of `to_dataframe`.

        Returns:
            DataQualityReport: Instance populated from DataFrame rows.
        """
        records = [
            DataQualityResult.model_validate(row) for row in df.to_dict("records")
        ]
        return cls(results=records)

    # ----------- all below to do with aggregating results following execution in Spark ----------

    def _aggregate_results(self) -> pd.DataFrame:
        """
        Aggregates partitioned results (e.g., from Spark) into a single DataFrame suitable for downstream analysis.
        This is because a single rule.evaluate() call would be split into separate partitions in Spark.

        Returns:
            pd.DataFrame: Combined DataFrame where duplicate results per rule are aggregated (e.g. sums, merged samples),
                recalculating pass rates and maintaining output consistency.
        """

        df = self.to_dataframe()
        df = add_records_passing(df)
        groupby_columns = self._get_groupby_columns()
        aggregation_dict = self._get_aggregation_dict()

        grouped_df = df.groupby(groupby_columns, dropna=False).agg(aggregation_dict)
        grouped_df = self._recalculate_pass_rate(grouped_df)

        grouped_df = self._sort_df_columns(grouped_df)
        grouped_df.replace(
            np.nan, None, inplace=True
        )  # consistency with output of to_dataframe() None rather than NaN
        return grouped_df

    def _sort_df_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Maintain the same order of columns in the dataframe as found in DataQualityResult"""

        # Noting that the columns we aggregate on will be in the index
        ordered_columns = [
            col for col in DataQualityResult.model_fields.keys() if col in df.columns
        ]
        return df[ordered_columns].reset_index()

    def _recalculate_pass_rate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Recalculates the pass rate following aggregation of each parition during spark execution"""
        df["pass_rate"] = df.agg(
            lambda row: calculate_pass_rate(
                row["records_passing"], row["records_evaluated"]
            ),
            axis=1,
        )
        df.drop(["records_passing"], axis=1, inplace=True)
        return df

    def _round_pass_rate(self, df: pd.DataFrame, decimals: int) -> pd.DataFrame:
        """Round the pass_rate column to the specified decimal places for readability.

        Decimals must be an integer between 1 and 10 (inclusive).
        """
        if not isinstance(decimals, int) or decimals < 1 or decimals > 10:
            raise ValueError(
                f"Parameter 'decimals' must be an integer between 1 and 10 (inclusive) for pass_rate. You provided: {decimals}"
            )
        df["pass_rate"] = (
            pd.to_numeric(df["pass_rate"], errors="coerce")
            .round(decimals)
            .where(pd.notnull(df["pass_rate"]), None)
        )
        return df

    @classmethod
    def _get_groupby_columns(cls) -> list[str]:
        """
        Return list of field names to group by when aggregating results (for use in spark when
        we get multiple results per partition).
        Excludes fields that are to be aggregated or are unreliable for groupby (e.g. row IDs).
        These values will all be identical per rule run.
        """
        groupby_columns = [
            "dataset_name",
            "dataset_id",
            "measurement_sample",
            "lifecycle_stage",
            "data_quality_dimension",
            "field",
            "rule_id",
            "rule_description",
            "rule_data",
        ]
        return groupby_columns

    @classmethod
    def _get_aggregation_dict(cls) -> dict:
        """Returns the aggregation dictionary that is used during pandas groupby, to combine
        partitioned results into one dataframe.
        """

        return {
            "records_evaluated": "sum",
            "records_failed_sample": aggregate_records_failed_samples,
            "records_failed_ids": aggregate_records_failed_ids,
            "records_passing": "sum",
            "measurement_time": "max",
        }


def _format_measurement_time(
    df: pd.DataFrame, measurement_time_format: str
) -> pd.DataFrame:
    """
    Formats the 'measurement_time' column in a DataFrame according to the provided strftime format string.

    Args:
        df (pd.DataFrame): DataFrame containing a 'measurement_time' column.
        measurement_time_format (str): Python strftime format string (e.g. '%Y-%m-%d %H:%M:%S.%f').

    Returns:
        pd.DataFrame: DataFrame with 'measurement_time' as formatted strings.

    Raises:
        ValueError: If the formatted string does not produce a valid datetime string representation."""
    test_dt = test_dt = pd.Timestamp(year=2025, month=1, day=1, hour=12)
    try:
        # Any string is valid in strftime, we need to ensure we output a valid datetime object
        _ = pd.Timestamp(test_dt.strftime(measurement_time_format))

    except Exception as e:
        raise ValueError(
            f"Error parsing the measurement_time_format you provided: '{measurement_time_format}' with dt.strftime(). "
            f"Exception: {e}\n"
            "Example of a suitable format: '%Y-%m-%d %H:%M:%S.%f'.\n"
            "See Python strftime directives: https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes"
        ) from e
    df["measurement_time"] = pd.to_datetime(df["measurement_time"]).dt.strftime(
        measurement_time_format
    )
    return df
