# (c) Crown Copyright GCHQ \n
import math
from typing import Literal

import pandas as pd
from pydantic import Field, field_validator

from gchq_data_quality.models import DamaFramework, DataQualityDimension
from gchq_data_quality.rules.base import BaseRule


class ValidityRegexRule(BaseRule):
    r"""
    Rule for validating string values against a regular expression.

    Considers only non-null entries, with additional missing-value patterns specified via
    `na_values`. A diagnostic sample of values failing the regex is returned if present.

    Attributes:
        field (str): Column to check for regex validity.
        regex_pattern (str): Regular expression pattern for validation.
        na_values (str | list[Any] | None): Additional values to treat as missing.
        data_quality_dimension (DamaFramework): Data quality dimension (Validity) by default.
        rule_id (str | None): Optional rule identifier.
        rule_description (str | None): Optional rule description.

    Methods:
        evaluate(data_source: pd.DataFrame | SparkDataFrame) -> DataQualityResult
            Evaluates the rule on the provided Pandas or Spark DataFrame and returns
            the metrics and diagnostics of the rule evaluation.

    Example:
        ```python
        >>> rule = ValidityRegexRule(
        ...     field="email",
        ...     regex_pattern=r'^[^@]+@[^@]+\.[^@]+$'
        ... )
        >>> result = rule.evaluate(df)

        >>> rule = ValidityRegexRule(
        ...     field="country_code",
        ...     regex_pattern=r'^[A-Z]{2}$'
        ... )
        >>> result = rule.evaluate(df)
        ```

    Note:
        To centrally manage and update regex patterns you can provide a separate YAML file containing named regex patterns
        (e.g., EMAIL_REGEX, POSTCODE_REGEX). Keys in this file are substituted in your
        main configuration files wherever referenced, enabling consistent and maintainable
        regex use.

        When storing regex patterns in YAML, always use single quotes ('pattern') rather than double quotes to ensure correct
        handling of typical regex escape characters, such as \d or \w.

            # regex_patterns.yaml
            EMAIL_REGEX: '^[^@]+@[^@]+\.[^@]+$'
            POSTCODE_REGEX: '^[A-Z]{2}[0-9]{2,3}\s?[0-9][A-Z]{2}$'

            # In your DQ config YAML, use the key in place of the regex pattern:
            rules:
            - function: validity_regex
                field: email
                regex_pattern: EMAIL_REGEX

            # Python code to load with substitution:
            >>> from gchq_data_quality.config import DataQualityConfig
            >>> dq_config = DataQualityConfig.from_yaml(
            ...     'your_config.yaml',
            ...     regex_yaml_path='regex_patterns.yaml'
            ... )

    Returns:
        DataQualityResult: Contains the validity score (`pass_rate`),
        sample and indices of failed records, number of evaluated records,
        and rule metadata. See DataQualityResult documentation for details.
    """

    function: Literal["validity_regex"] = "validity_regex"
    regex_pattern: str = Field(
        ..., description="Regular expression pattern to validate against"
    )
    data_quality_dimension: DataQualityDimension = Field(default=DamaFramework.Validity)

    def _coerce_dataframe_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """coerce to a string for regex, we need to preserve NULL"""
        return df.where(df.isnull(), df.astype(str))

    def _get_records_passing_mask_pandas(self, df: pd.DataFrame) -> pd.Series:
        """Records pass if they match the regex"""
        passing_mask = df[self.field].str.match(self.regex_pattern)

        return passing_mask


class ValidityNumericalRangeRule(BaseRule):
    """
    Rule for validating numerical values against a specified range.

    Considers only non-null values; values outside the range or failing coercion to
    numeric are considered invalid. Diagnostic samples and record indices are
    returned for values outside the allowed range.

    Attributes:
        field (str): Column to check for numerical range validity.
        min_value (float): Minimum allowed value (inclusive; defaults to -infinity).
        max_value (float): Maximum allowed value (inclusive; defaults to +infinity).
        na_values (str | list[Any] | None): Additional values to treat as missing.
        data_quality_dimension (DamaFramework): Data quality dimension (Validity).
        rule_id (str | None): Optional rule identifier.
        rule_description (str | None): Optional rule description.

    Methods:
        evaluate(data_source: pd.DataFrame | SparkDataFrame) -> DataQualityResult
            Evaluates the rule on the provided Pandas or Spark DataFrame and returns
            the metrics and diagnostics of the rule evaluation.

    Example:
        ```python
        >>> rule = ValidityNumericalRangeRule(
        ...     field="age",
        ...     min_value=0,
        ...     max_value=120
        ... )
        >>> result = rule.evaluate(df)

        # no upper limit
        >>> rule = ValidityNumericalRangeRule(
        ...     field="temp_c",
        ...     min_value=0,
        ...     na_values=-999
        ... )
        >>> result = rule.evaluate(df)

        # no lower limit
        >>> rule = ValidityNumericalRangeRule(
        ...     field="score",
        ...     max_value=100,
        ...     na_values=['missing', 'N/A']
        ... )
        >>> result = rule.evaluate(df)
        ```

    Returns:
        DataQualityResult: Contains the validity score (`pass_rate`),
        sample and indices of failed records, total records evaluated,
        and rule metadata. See DataQualityResult documentation for details.
    """

    function: Literal["validity_numerical_range"] = "validity_numerical_range"
    min_value: float = Field(default=-math.inf, description="Minimum valid value")
    max_value: float = Field(default=math.inf, description="Maximum valid value")
    data_quality_dimension: DataQualityDimension = Field(default=DamaFramework.Validity)

    @field_validator("min_value", mode="before")
    @classmethod
    def _set_none_min_to_infinity(cls, value: float | None) -> float:
        if value is None:
            return -math.inf
        else:
            return value

    @field_validator("max_value", mode="before")
    @classmethod
    def _set_none_max_to_infinity(cls, value: float | None) -> float:
        if value is None:
            return math.inf
        else:
            return value

    def _coerce_dataframe_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """Coerce to numerical type"""
        return df.apply(pd.to_numeric, errors="coerce")

    def _get_records_passing_mask_pandas(self, df: pd.DataFrame) -> pd.Series:
        """records pass if they are between the min and max value"""

        records_passing_mask = (df[self.field] >= self.min_value) & (
            df[self.field] <= self.max_value
        )
        return records_passing_mask
