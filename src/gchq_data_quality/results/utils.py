# (c) Crown Copyright GCHQ \n
"""This module provides utility functions for result aggregation and manipulation
within the data quality framework. It is intended for internal use by data quality
rule and configuration classes (e.g., Rule.evaluate(df), DataQualityConfig.execute(df)).

Functions in this module are designed to operate independently of Pydantic models or
explicit schema knowledge, to avoid circular import dependencies.


Note:
    End users should not call these functions directly. They are integrated into
    the models and configuration classes that orchestrate data quality evaluation.
"""

import pandas as pd

from gchq_data_quality.globals import SampleConfig


def add_records_passing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a 'records_passing' column to the DataFrame, calculated as
    the product of 'records_evaluated' and 'pass_rate' per row.

    Args:
        df (pd.DataFrame): DataFrame containing columns 'records_evaluated' (int)
            and 'pass_rate' (float, typically result of rule.evaluate()).

    Returns:
        pd.DataFrame: The input DataFrame with an added 'records_passing' column

    """

    df["records_passing"] = df["records_evaluated"] * df["pass_rate"]
    return df


def coerce_nan_to_none(records_failed_sample: list[dict]) -> list[dict]:
    """To avoid JSON serialisation errors, we need to remove pd.NaT and np.Nan
    in the records_failed_sample values

    Such that [{"columnA" : nan}] -> [{"columnA" : None}]

    Args:
        records_failed_sample (list[dict]): The records_failed_sample value from within a DataQualityResult

    Returns:
        list[dict]: A modified records_failed_sample list with any value that is pd.isna() coerced to None
    """

    for _dict in records_failed_sample:
        for key, value in _dict.items():
            if pd.isna(value):
                _dict[key] = None

    return records_failed_sample


def _dedupe_and_drop_na(series: pd.Series) -> pd.Series:
    return series.dropna().drop_duplicates()


def _aggregate_lists_in_series(series: pd.Series) -> list | None:
    """
    Combines all list elements from a pandas Series into a single list, ignoring any None or non-list values.

    Args:
        series (pd.Series): Series in which each value is either a list (to be concatenated) or None.

    Returns:
        list | None: A concatenated list of all elements, or None if there are no list values present.

    """
    all_samples = [
        item for sample in series if isinstance(sample, list) for item in sample
    ]
    return all_samples if all_samples else None


def aggregate_records_failed_samples(
    records_failed_sample_series: pd.Series,
) -> list[dict] | None:
    """
    Aggregates sampled failed record details (as lists of dicts) from multiple partitions
    into a single list, applying a global limit as set in output_config.

    Used internally to construct rule result summaries after evaluating:
        Rule.evaluate(df) or config.execute(df)

    Args:
        records_failed_sample_series (pd.Series):
            Series where each value is a list of dicts (failed record samples) or None.

    Returns:
        list[dict] | None: Concatenated sample list (up to FAILED_RECORDS_IN_SAMPLE), or None.
    """

    unique_rows = _dedupe_and_drop_na(records_failed_sample_series)
    all_samples = _aggregate_lists_in_series(unique_rows)

    if all_samples:
        return all_samples[: SampleConfig.RECORDS_FAILED_SAMPLE_SIZE]
    return None


def aggregate_records_failed_ids(records_failed_ids_series: pd.Series) -> list | None:
    """
    Aggregates failed record IDs (as lists) from multiple partitions, removing
    duplicates and converting to a single, truncated list. Used during execution
    in Spark.

    Intended for integration within Rule or Config evaluation results:
        Rule.evaluate(df) or config.execute(df)

    Args:
        records_failed_ids_series (pd.Series):
            Series where each value is a list of integer or string IDs, or None.

    Returns:
        list | None: List of unique failed record IDs (up to RECORDS_FAILED_SAMPLE_SIZE), or None.

    """
    unique_rows = _dedupe_and_drop_na(records_failed_ids_series)
    all_samples = _aggregate_lists_in_series(unique_rows)

    if all_samples:
        return all_samples[: SampleConfig.RECORDS_FAILED_SAMPLE_SIZE]
    return None


def records_failed_ids_are_int(records_failed_ids: list) -> bool:
    """
    Checks whether all provided failed record IDs are integers. This is used
    prior to shifting these values by an integer amount during
    e.g. DataQualityReport.to_dataframe(shift_records_failed_ids:int=2)


    Args:
        records_failed_ids (list): List containing record IDs.

    Returns:
        bool: True if all IDs are integers, False otherwise.
    """

    if all([isinstance(row_number, int) for row_number in records_failed_ids]):
        return True
    else:
        return False


def shift_records_failed_ids(records_failed_ids: list, shift: int = 0) -> list:
    """
    Shifts integer-based failed record IDs by a specified value.
    This is commonly used when adjusting DataFrame row indices to align with
    real-world file formats (e.g., Excel data rows with a header start at index 2, not 0).

    Args:
        records_failed_ids (list): List of integer record IDs that failed a rule.
        shift (int, optional): The offset to add to each ID. Default is 0.

    Returns:
        list: List of shifted record IDs if input are integers;
            original list otherwise.

    Example:
        ```python
        >>> shift_records_failed_ids([0, 1, 2], shift=2)
        [2, 3, 4]
        ```

    Note: Called by Pydantic result model when rendering end-user reports.
    """

    if records_failed_ids and records_failed_ids_are_int(records_failed_ids):
        return [row_number + shift for row_number in records_failed_ids]
    else:
        return records_failed_ids
