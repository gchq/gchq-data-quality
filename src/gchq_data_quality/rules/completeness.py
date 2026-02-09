# (c) Crown Copyright GCHQ \n
from typing import Literal

import pandas as pd
from pydantic import Field

from gchq_data_quality.models import DamaFramework, DataQualityDimension
from gchq_data_quality.rules.base import BaseRule


class CompletenessRule(BaseRule):
    """
    Rule to calculate the completeness score for a field.

    Completeness is measured as the proportion of non-null values in the specified
    column. Values specified in `na_values` are converted to nulls prior to calculation.

    Attributes:
        field (str): The column name to assess.
        na_values (str | list[Any] | None): Additional indicators to treat as missing.
        rule_id (str | None): Optional identifier for the rule.
        rule_description (str | None): Optional description of the rule.

    Methods:
        evaluate(data_source: pd.DataFrame | SparkDataFrame) -> DataQualityResult
            Evaluates completeness for the chosen fields on a Pandas or Spark DataFrame.
            Returns the metrics and diagnostics of the rule evaluation.

    Example:
        ```python
        >>> rule = CompletenessRule(field="column_name")
        >>> result = rule.evaluate(df)
        >>> print(result.pass_rate)

        >>> rule = CompletenessRule(field="column_name", na_values="missing")
        >>> result = rule.evaluate(df)
        ```

    Returns:
        DataQualityResult: Contains completeness score (`pass_rate`), field name,
        number of records evaluated, and rule metadata. See DataQualityResult documentation
        for further attribute details.
    """

    function: Literal["completeness"] = "completeness"
    data_quality_dimension: DataQualityDimension = Field(
        default=DamaFramework.Completeness
    )
    skip_if_null: Literal["all", "any", "never"] = Field(
        default="never", description="...", frozen=True
    )
    # The default _get_records_evaluated_pandas of non-null values is fine (BaseRule)

    def _get_records_passing_mask_pandas(self, df: pd.DataFrame) -> pd.Series:
        """
        Determines the number of non-null values in the df
        """
        return df[self.field].notnull()
