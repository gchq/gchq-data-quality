# (c) Crown Copyright GCHQ \n
from typing import Literal, Self

import pandas as pd
from pydantic import Field, field_validator

from gchq_data_quality.models import DamaFramework, DataQualityDimension
from gchq_data_quality.rules.base import BaseRule
from gchq_data_quality.rules.utils.rules_utils import (
    evaluate_bool_expression,
    extract_columns_from_expression,
)


class ConsistencyRule(BaseRule):
    """
    Rule for evaluating data consistency based on boolean expressions (with an optional condition).

    Expressions may use any valid Pandas eval syntax that returns a boolean result. Backticks are required around all column names.
    Nulls and additional na_values are handled according to the skip policy.

    Attributes:
        field (str): The column to check for consistency.
        expression (str | dict[str, str]): A boolean expression, or a conditional {'if', 'then'} dictionary (with backticks for column names).
        skip_if_null (Literal['all', 'any', 'never']): Controls row skipping for null values in relevant columns.
        na_values (str | list[Any] | None): Additional values considered as missing.
        data_quality_dimension (DamaFramework): Associated data quality dimension - you may want to override it in this rule.
        rule_id (str | None): Optional identifier for the rule.
        rule_description (str | None): Optional description of the rule.

    Methods:
        evaluate(data_source: pd.DataFrame | SparkDataFrame) -> DataQualityResult
            Evaluates the rule on the provided Pandas or Spark DataFrame and returns
            the metrics and diagnostics of the rule evaluation.

    Example:
        ```python
        >>> rule = ConsistencyRule(
        ...     field="score",
        ...     expression="`score` >= 50"
        ... )
        >>> result = rule.evaluate(df)

        >>> rule = ConsistencyRule(
        ...     field="completion_date",
        ...     expression={"if": "`status` == 'completed'", "then": "`completion_date`.notnull()"},
                data_quality_dimension='Completeness' # you can override the DAMA Dimension
        ... )
        >>> result = rule.evaluate(df)

        # all series .str. methods are available
        >>> rule = ConsistencyRule(
        ...     field="postcode",
        ...     expression={
        ...         "if": "`country` == 'UK'",
        ...         "then": "`postcode`.str.match(r'^[A-Z]{2}[0-9]{2}$')"
        ...     }
        ... )
        >>> result = rule.evaluate(df)

        # Date parts and arithmetic using .dt accessor
        >>> rule = ConsistencyRule(
        ...     field="report_year",
        ...     expression="`report_date`.dt.year == `report_year`"
        ... )
        >>> result = rule.evaluate(df)

        # Boolean logic (AND, OR, NOT) with grouping and comparisons
        >>> rule = ConsistencyRule(
        ...     field="flag",
        ...     expression="(`score` > 90) & ((`status` == 'active') | ~`is_archived`)"
        ... )
        >>> result = rule.evaluate(df)

        # Using mathematical operations
        >>> rule = ConsistencyRule(
        ...     field="predicted",
        ...     expression="abs(`actual` - `predicted`) < 10"
        ... )
        >>> result = rule.evaluate(df)
        ```

    Returns:
        DataQualityResult: An object containing the consistency score (`pass_rate`),
        number of records evaluated, a sample of inconsistent records, and details of failed row indices.
        See DataQualityResult documentation for full attribute descriptions.
    """

    function: Literal["consistency"] = "consistency"
    expression: str | dict[str, str] = Field(
        ...,
        description="A pandas eval compatible expression. Either a string for simple comparison or dict with 'if' and 'then' keys, where 'if' will contain a logical clause.",
    )
    skip_if_null: Literal["all", "any", "never"] = Field(
        default="all", description="...", frozen=False
    )
    data_quality_dimension: DataQualityDimension = Field(
        default=DamaFramework.Consistency
    )

    @field_validator("expression")
    @classmethod
    def validate_rule_format(cls, value: str | dict) -> str | dict:
        """Check we have both an 'if' and 'then' key if it's a dictionary."""
        if isinstance(value, dict):
            if not all(key in value for key in ["if", "then"]):
                raise ValueError(
                    f"Expression dict must contain both 'if' and 'then' keys, you have {value.keys()}"
                )
        return value

    def _get_columns_used_pandas(self) -> list:
        """Get the list of columns used in any part of the expression and the field.
        The field *should* be present in at least one of the expressions, but there
        may be unexpected edge cases where it isn't, so we'll explicitly add it to be sure."""
        if isinstance(self.expression, str):
            cols_in_expression = extract_columns_from_expression(self.expression)
        else:  # dictionary with if, then keys.
            columns_in_if_statement = extract_columns_from_expression(
                self.expression["if"]
            )

            columns_in_then_statement = extract_columns_from_expression(
                self.expression["then"]
            )
            cols_in_expression = list(
                set(columns_in_if_statement + columns_in_then_statement)
            )

        return list(set(cols_in_expression + [self.field]))

    def _get_records_evaluated_mask_pandas(self, df: pd.DataFrame) -> pd.Series:
        """we evaluate records that we do not skip (based on NULL logic across all relevant columns).
        if we have an if statement, then we additionally constrain our evaluated records
        by only those that pass the if statement.
        """

        skip_mask = self._get_skip_if_null_mask(df)
        if isinstance(self.expression, str):
            return ~skip_mask
        else:  # it will be a dictionary with if, then keys
            if_statement_true = evaluate_bool_expression(df, self.expression["if"])
            return if_statement_true & ~skip_mask

    def _get_records_passing_mask_pandas(self, df: pd.DataFrame) -> pd.Series:
        """For a record to pass a consistency check:
        1. if it's a simple string like X > 3, we just evaluate that boolean expression
        2. if it's a dictionary, then it passes if the 'then' statement evalutes to true.
        Noting we correctly calculate the pass_rate by combining this with the records_evaluated mask
        which takes into account the corresponding 'if' statement.

        i.e. the number of passing records is (passing_mask & evaluated_mask), where evaluated_mask
        is reliant on if_mask being True."""

        if isinstance(self.expression, str):
            return evaluate_bool_expression(df, self.expression)
        else:  # dictionary of format  {'if': , 'then':}
            return evaluate_bool_expression(df, self.expression["then"])

    def _get_spark_safe_rule(self) -> Self:
        """We override the default behaviour as we additionally need to make
        the expression spark_safe - in case columns refer to nested data."""
        from gchq_data_quality.spark.utils.rules_utils import (
            get_spark_safe_column_name,
            get_spark_safe_expression,
        )

        rule_copy = self.model_copy()
        rule_copy.field = get_spark_safe_column_name(self.field)
        rule_copy.expression = get_spark_safe_expression(self.expression)

        return rule_copy
