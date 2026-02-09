# (c) Crown Copyright GCHQ \n
from typing import Any, Literal

import pandas as pd
from pydantic import Field

from gchq_data_quality.models import DamaFramework, DataQualityDimension
from gchq_data_quality.rules.base import BaseRule


class AccuracyRule(BaseRule):
    """
    Rule to check if values meet a list of valid (or invalid) values.

    Skips NULLs, including those recognised via `na_values`. Instantiate this class and
    call `.evaluate(df)` to assess data quality for the chosen column.

    Attributes:
        field (str): The column to check for accuracy.
        valid_values (list[Any]): The set of acceptable values for the field.
        inverse (bool): If True, values in `valid_values` are considered invalid (exclusion list).
        na_values (str | list[Any] | None): Additional indicators to treat as missing values.
        rule_id (str | None): Optional identifier for the rule.
        rule_description (str | None): Optional description of the rule.

    Methods:
        evaluate(data_source: pd.DataFrame | SparkDataFrame) -> DataQualityResult
            Evaluates the rule on the provided Pandas or Spark DataFrame and returns
            the metrics and diagnostics of the rule evaluation.

    Example:
        ```python
        >>> rule = AccuracyRule(field="category", valid_values=["A", "B", "C"])
        >>> result = rule.evaluate(df)
        >>> print(result.pass_rate)
        >>> print(result.records_failed_ids)

        >>> rule = AccuracyRule(
        ...     field="department",
        ...     valid_values=["HR", "IT", "Sales"],
        ...     na_values=["N/A", "N/K"]
        ... )
        >>> result = rule.evaluate(df)

        >>> rule = AccuracyRule(
        ...     field="status",
        ...     valid_values=["expired", "deleted"],
        ...     inverse=True # value must NOT be expired or deleted
        ... )
        >>> result = rule.evaluate(df)
        ```

    Returns:
        DataQualityResult: An object containing the accuracy score (pass_rate),
        the indices of failed rows (records_failed_ids), a sample of failed values
        (records_failed_sample), the number of records evaluated, and further rule metadata.
        See DataQualityResult documentation for full details.
    """

    function: Literal["accuracy"] = "accuracy"
    valid_values: list[Any] = Field(..., description="List of valid values")
    inverse: bool | None = Field(
        default=False,
        description="If true, checks that values are NOT in valid_values - effectively makes the list a set of invalid_values",
    )
    data_quality_dimension: DataQualityDimension = Field(default=DamaFramework.Accuracy)

    # The default _get_records_evaluated_pandas of non-null values is fine (BaseRule)

    def _get_records_passing_mask_pandas(self, df: pd.DataFrame) -> pd.Series:
        """
        Determines the number of records that are one of valid_values,
        unless inverse = True, in which case the number that are NOT in valid_values
        """
        accurate_mask = df[self.field].isin(self.valid_values)
        if self.inverse:
            accurate_mask = ~accurate_mask
        return accurate_mask
