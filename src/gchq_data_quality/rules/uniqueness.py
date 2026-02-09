# (c) Crown Copyright GCHQ \n
"""
This module defines the UniquenessRule, which measures the proportion of unique values within a specified column of a DataFrame.
It is useful for evaluating the distinctness of identifiers such as IDs.

All standard parameters and evaluation logic are inherited from BaseRule.
"""

from typing import TYPE_CHECKING, Literal

import pandas as pd
from pydantic import Field

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame

from gchq_data_quality.globals import SampleConfig
from gchq_data_quality.models import DamaFramework, DataQualityDimension
from gchq_data_quality.results.models import DataQualityResult
from gchq_data_quality.rules.base import BaseRule
from gchq_data_quality.rules.utils.rules_utils import calculate_pass_rate


class UniquenessRule(BaseRule):
    """
    Rule for assessing uniqueness in a column.

    Measures the proportion of unique, non-null values in a specified column. This is
    useful for checking distinct identifiers or reference keys. Additional null-like
    values can be specified via `na_values`.

    Attributes:
        field (str): Column to evaluate for uniqueness.
        na_values (Any | list[Any] | None): Values to treat as missing.
        data_quality_dimension (DamaFramework): Data quality dimension (Uniqueness).
        rule_id (str | None): Optional rule identifier.
        rule_description (str | None): Optional description for this rule.

    Methods:
        evaluate(data_source: pd.DataFrame | SparkDataFrame) -> DataQualityResult
            Evaluates the rule on the provided Pandas or Spark DataFrame and returns
            the metrics and diagnostics of the rule evaluation.

    Example:
        ```python
        >>> import pandas as pd
        >>> from gchq_data_quality.rules.uniqueness import UniquenessRule
        >>> df = pd.DataFrame({'id': [1, 2, 3, 3, None]})

        # Basic uniqueness check
        >>> rule = UniquenessRule(field='id')
        >>> result = rule.evaluate(df)
        >>> print(result.pass_rate)
        0.75

        # Specify additional NA values
        >>> rule = UniquenessRule(field='id', na_values=[-1])
        >>> df = pd.DataFrame({'id': [1, 2, -1, 3, 3]})
        >>> result = rule.evaluate(df)
        >>> print(result.pass_rate)
        0.75
        ```

    Note:
        The pass_rate metric is calculated as (number of unique values) /
        (number of non-null records). Therefore, if every value in the column
        appears exactly twice, pass_rate will be 0.5 (not 0.0!). For columns with even more
        duplication, pass_rate will decrease and approach zero as the number of
        unique values becomes small relative to the number of total records.
        Only if every record is identical will pass_rate be 1 / N (where N is the number
        of records).

    Returns:
        DataQualityResult: Contains the uniqueness score (`pass_rate`), identifiers for failed records,
        a sample of duplicate values, the number of records evaluated, and rule metadata.
        See DataQualityResult documentation for further attribute details.

    """

    function: Literal["uniqueness"] = "uniqueness"
    data_quality_dimension: DataQualityDimension = Field(
        default=DamaFramework.Uniqueness
    )

    # The default _get_records_evaluated_pandas of non-null values is fine (BaseRule)

    def _get_records_passing_mask_pandas(self, df: pd.DataFrame) -> pd.Series:
        """
        Creates a boolean mask indicating whether each record in the specified column is unique.

        A record 'passes' if its value has not appeared earlier in the column, meaning the
        first instance of each value is considered unique (True), and all subsequent
        duplicates are marked as not unique (False). Nulls are handled separately via na_values.

        Args:
            df (pd.DataFrame): The DataFrame to evaluate.

        Returns:
            pd.Series: Boolean mask with True for unique records and False for duplicates.
        """
        return ~df[self.field].duplicated()

    def _evaluate_in_spark(self, spark_df: "SparkDataFrame") -> DataQualityResult:
        """
        Evaluates uniqueness of the specified column in the Spark DataFrame.

        This method calculates the proportion of unique (non-null and non-duplicate)
        values in the target column. It must operate over the entire DataFrame in one
        operation, because computing uniqueness within Spark partitions (such as when using
        mapInPandas) will not yield correct global uniqueness: duplicates across partitions
        cannot be detected.

        Args:
            spark_df (SparkDataFrame): The Spark DataFrame containing the column to check.

        Returns:
            DataQualityResult: The result object with the uniqueness score (`pass_rate`),
            details of duplicate values, count of evaluated records, and relevant rule metadata.
        """
        import pyspark.sql.functions as F  # noqa: N812

        from gchq_data_quality.spark.utils.rules_utils import (
            replace_na_values_spark,
        )

        if self.na_values is not None:
            spark_df = replace_na_values_spark(
                spark_df,
                columns=[self.field],
                na_values=self.na_values,
            )

        records_evaluated_df = spark_df.filter(F.col(self.field).isNotNull())
        records_evaluated = records_evaluated_df.count()

        records_passing = records_evaluated_df.select(self.field).distinct().count()
        pass_rate = calculate_pass_rate(
            records_passing=records_passing, records_evaluated=records_evaluated
        )

        dq_result = DataQualityResult(
            pass_rate=pass_rate,
            field=self.field,
            data_quality_dimension=self.data_quality_dimension,
            rule_id=self.rule_id,
            rule_description=self.rule_description,
            rule_data=self.to_json(),
            records_evaluated=records_evaluated,
        )

        if self._require_failed_records_sample(pass_rate):
            records_failed_df = (
                records_evaluated_df.groupBy(self.field)
                .agg(F.count("*").alias("count"))
                .filter(F.col("count") > 1)
            )
            records_failed_sample = [
                row[self.field]
                for row in records_failed_df.limit(
                    SampleConfig.RECORDS_FAILED_SAMPLE_SIZE
                ).collect()
            ]

            records_as_dict = [{self.field: value} for value in records_failed_sample]
            dq_result._set_records_failed_sample(records_as_dict)

        return dq_result
