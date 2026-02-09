# (c) Crown Copyright GCHQ \n
"""
Base classes and default behaviour for data quality rules.

This module provides the abstract BaseRule class and supporting logic for evaluating
data quality on tabular data (Pandas, Spark). Default behaviours,
such as column selection, null handling, data coercion, and pass/fail mask logic,
are defined here and inherited by specific rule implementations (e.g., AccuracyRule,
CompletenessRule).

"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any, Literal, Self, overload
from warnings import warn

import pandas as pd

# Do not force users to have access to pyspark or elasticsearch unless required

try:
    from elasticsearch import Elasticsearch

    _has_elasticsearch = True
except ImportError:
    _has_elasticsearch = False

try:
    from pyspark.sql import DataFrame as SparkDataFrame

    _has_pyspark = True
except ImportError:
    _has_pyspark = False


from pydantic import (
    Field,
)

from gchq_data_quality.globals import SampleConfig
from gchq_data_quality.models import (
    DataQualityBaseModel,
    DataQualityDimension,
)
from gchq_data_quality.results.models import DataQualityResult
from gchq_data_quality.rules.utils.rules_utils import (
    calculate_pass_rate,
    ensure_columns_exist_pandas,
    get_records_failed_ids,
    replace_na_values_pandas,
)


class BaseRule(DataQualityBaseModel, ABC):
    """
    Abstract base class for data quality rule definitions.

    Not intended for direct use. Use a Subclass with a specific rule type (e.g., AccuracyRule,
    CompletenessRule) for configuration or execution of data quality checks. BaseRule
    handles all generic configuration and evaluation steps, with rule-specific logic
    implemented via subclass overrides.

    Attributes:
        field (str): Column to check for rule evaluation.
        rule_id (str | None): Optional identifier for this rule.
        rule_description (str | None): Optional summary or explanation of the rule.
        na_values (str | int | float | list[Any] | None): Values to treat as NULL.
        skip_if_null (Literal["all", "any", "never"]): Controls what records are skipped due to nulls.
        data_quality_dimension (DamaFramework): Linked DAMA data quality dimension.

    Methods:
        evaluate(data_source: pd.DataFrame | SparkDataFrame | Elasticsearch) -> DataQualityResult
            Applies the rule to source data and returns evaluation metrics and diagnostics.

    Note:
        This base class should not be instantiated directly. Use a rule subclass for
        actual configuration or evaluation.

    Returns:
        DataQualityResult: Contains metrics of evaluation such as pass rate,
        evaluated record count, indices/sample of failed records, and rule metadata.
        See DataQualityResult documentation for details.
    """

    field: str = Field(..., description="Column to check")
    rule_id: str | None = Field(default=None, description="Identifier for this rule")
    rule_description: str | None = Field(
        default=None, description="Description of the rule"
    )

    na_values: str | int | float | list[Any] | None = Field(
        default=None, description="Additional values to treat as null"
    )
    skip_if_null: Literal["all", "any", "never"] = Field(
        default="any",
        description="Controls which rows are skipped that contain null values. If 'all' then it will only skip if all columns used are NULL."
        "most rules this will just apply to the 'field' column, but some like TimelinessRelativeRule can use more than one column."
        "If values aren't skipped, then NULL values are passed into the calculations so be cautious as to what you allow through"
        " as 3 > pd.NA = <NA> ",
    )
    data_quality_dimension: DataQualityDimension = Field(
        ..., description="The Dama dimension for each rule"
    )

    @overload
    def evaluate(self, data_source: pd.DataFrame) -> DataQualityResult: ...
    @overload
    def evaluate(self, data_source: SparkDataFrame) -> DataQualityResult: ...
    @overload
    def evaluate(
        self,
        data_source: Elasticsearch,
        index_name: str = ...,
        query: dict | None = ...,
    ) -> DataQualityResult: ...

    def evaluate(
        self,
        data_source: pd.DataFrame | SparkDataFrame | Elasticsearch,
        index_name: str = "",
        query: dict | None = None,
    ):
        """
        Evaluates this rule against the provided data source.

        Supports both Pandas and Spark DataFrames as input. Applies all
        rule configuration, handles nulls and data coercion, and computes
        relevant data quality metrics. If an Elasticsearch index and client
        are supplied, an error is raised unless that backend is implemented.
        Currently not implemented.

        Args:
            data_source (pd.DataFrame | SparkDataFrame | Elasticsearch): The data to evaluate—
                can be a Pandas DataFrame, a Spark DataFrame, or an Elasticsearch client.
            index_name (str, optional): Required if evaluating with Elasticsearch; the index to check.
            query (dict, optional): Required if evaluating with Elaticsearch, defaults to a query that matches all documents

        Returns:
            DataQualityResult: Contains the metrics and diagnostics of rule evaluation,
            including pass rate, number of records evaluated, indices and sample of failed records,
            and rule metadata. See DataQualityResult documentation for details.

        Raises:
            ValueError: If an unsupported data source is provided.
            NotImplementedError: If Elasticsearch evaluation is requested but not supported.
        """

        if isinstance(data_source, pd.DataFrame):
            return self._evaluate_in_pandas(data_source)
        elif _has_pyspark and isinstance(data_source, SparkDataFrame):  # pyright: ignore[reportPossiblyUnboundVariable]
            return self._evaluate_in_spark(data_source)  # pyright: ignore[reportArgumentType]
        elif _has_elasticsearch and isinstance(data_source, Elasticsearch):  # pyright: ignore[reportPossiblyUnboundVariable]
            return self._evaluate_in_elastic(data_source, index_name, query)  # pyright: ignore[reportArgumentType]
        else:
            raise ValueError(
                "You must pass in either a pandas or spark dataframe or an Elasticsearch client with an index name."
                "If you are passing in a spark dataframe or elasticsearch client, ensure these package are installed. pip install elasticsearch; pip install pyspark - "
                "our code will run even if these packages are not present."
            )

    def _evaluate_in_pandas(self, df: pd.DataFrame) -> DataQualityResult:
        """
        Evaluates the rule against the provided DataFrame.

        Performs field existence check, handles NA values and coercion calculates number of records evaluated and passing,
        computes pass rate, and includes a sample of failed records if required.
        A subset of the steps below can be overriden to give any inherited rule the desirved behaviour,
        without having to completely override the evaluate() function itself.

        Args:
            df (pd.DataFrame): The DataFrame to evaluate

        Returns:
            DataQualityResult: A summary of data quality metrics for the rule such as
            records evaluated, pass rate, and details of failed records if required.
        """

        columns_used = self._get_columns_used_pandas()
        ensure_columns_exist_pandas(df, columns_used)
        df = self._copy_and_subset_dataframe(df, columns_used)

        # df now only contains the subset of columns required (by default, just df[field]) and has been copied
        df = self._handle_dataframe_coercion(df)
        df = self._handle_na_values_pandas(df, columns_used, self.na_values)

        # the defining logic in every rule is what records are passed and which are evaluated
        records_evaluated = self._get_records_evaluated_pandas(df)
        records_passing = self._get_records_passing_pandas(df)
        pass_rate = calculate_pass_rate(records_passing, records_evaluated)

        data_quality_result = DataQualityResult(
            field=self.field,
            data_quality_dimension=self.data_quality_dimension,
            records_evaluated=records_evaluated,
            pass_rate=pass_rate,
            rule_id=self.rule_id,
            rule_description=self.rule_description,
            rule_data=self.to_json(),
        )

        if self._require_failed_records_sample(pass_rate):
            records_failed_mask = self._get_records_failed_mask_pandas(df)
            records_failed_values = self._get_records_failed_pandas(df)
            data_quality_result._set_records_failed_sample(records_failed_values)
            data_quality_result.records_failed_ids = get_records_failed_ids(
                df, records_failed_mask, SampleConfig.RECORDS_FAILED_SAMPLE_SIZE
            )

        return data_quality_result

    # --------------- Pandas helper functions ---------------

    @abstractmethod
    def _get_records_passing_mask_pandas(self, df: pd.DataFrame) -> pd.Series:
        """The bool mask of what records are passing (i.e. this function is the main way we define our data quality rules), this is also an AND with
        the records_evaluated_mask by definition, as we cannot pass a record
        if it has not been evaluated."""

        pass  # pragma: no cover

    def _get_records_evaluated_mask_pandas(self, df: pd.DataFrame) -> pd.Series:
        """The bool mask of whether a record is being evaluated.
        The majority of rules will not evaluate against records that are NULL
        With the exception of the CompletenessRule. So the default behaviour
        is evaluate NON null values.
        """

        skip_mask = self._get_skip_if_null_mask(df)
        return ~skip_mask

    def _get_skip_if_null_mask(self, df: pd.DataFrame) -> pd.Series:
        """Return mask for records to skip based on self.skip_if_null."""

        if self.skip_if_null == "any":
            return df.isnull().any(axis=1)
        elif self.skip_if_null == "all":
            return df.isnull().all(axis=1)
        elif self.skip_if_null == "never":
            return pd.Series(False, index=df.index)
        else:
            raise ValueError(f"Unexpected skip_if_null value: {self.skip_if_null}")
        # although pydantic will not allow any other option so this ValueError should never raise

    def _get_columns_used_pandas(self) -> list[str]:
        """The columns used in evaluting the rule, defaults to just the field, but other rules
        such as consistency may use more than one column and will override this."""
        return [self.field]

    def _copy_and_subset_dataframe(
        self, df: pd.DataFrame, columns_used: list[str]
    ) -> pd.DataFrame:
        """Copies the dataframe to avoid later mutations when we replace NA values
        or coerce to a different data type.

        Also ensures the dataframe columns are kept in the same order as the orginal df"""

        sorted_cols = sorted(columns_used, key=lambda x: list(df.columns).index(x))
        return df[sorted_cols].copy()

    def _handle_dataframe_coercion(self, df: pd.DataFrame) -> pd.DataFrame:
        """Coerce the dataframe to a new datatype (if required).
        We will also check if the null count changes upon coercion and raise a warning with the user"""

        original_null_counts = self._get_null_counts_all_columns(df)
        df_coerced = self._coerce_dataframe_type(df)
        new_null_counts = self._get_null_counts_all_columns(df_coerced)
        self._warn_if_null_counts_different(original_null_counts, new_null_counts)
        return df_coerced

    def _get_null_counts_all_columns(self, df: pd.DataFrame) -> dict[str, int]:
        """Goes through each column and calculates the null count.

        Returns:
            A dictionary of {column_name : null_count} e.g. {'name' : 7, 'age' : 0}"""

        null_counts = dict()
        for col in df.columns:
            null_counts[col] = self._get_null_count(df, col)

        return null_counts

    def _get_null_count(self, df: pd.DataFrame, field: str) -> int:
        return df[field].isnull().sum()

    def _warn_if_null_counts_different(
        self, original_null_counts: dict[str, int], new_null_counts: dict[str, int]
    ) -> None:
        """Compares the null counts between the original and new (the keys will be the same), if the new
        has more nulls, raise a warning and mention the column. Typically something we do during coercion
        to a new datatype."""

        for column_name in original_null_counts:
            if new_null_counts[column_name] > original_null_counts[column_name]:
                warn(
                    f"Column '{column_name}' has {new_null_counts[column_name] - original_null_counts[column_name]} additional NULL values after coercion",
                    stacklevel=2,
                )

    def _coerce_dataframe_type(self, df: pd.DataFrame) -> pd.DataFrame:
        """Some rules require values to be coerced to a different data type.
        Timeliness > UTC datetime,
        ValiditiyNumericalRange > numeric

        This function handles coercing to the relevant data type
        for the rule. Override if needed, the default behaviour is no coercion

        Returns:
            pd.DataFrame: If no coercion, the original df. If coerced, a modifed dataframe
        """
        return df

    def _handle_na_values_pandas(
        self,
        df: pd.DataFrame,
        columns_used: list[str],
        na_values: str | float | int | list[Any] | None,
    ) -> pd.DataFrame:
        """Replace specified values in a DataFrame with pd.NA if na_values is provided.

        Args:
            df (pd.DataFrame): Input dataframe.
            columns_used (list): Columns to scan for null-like values.
            na_values (list): List of values to consider as missing.

        Returns:
            pd.DataFrame: A dataframe where the specified values are replaced with pd.NA, or the original if na_values is None.
        """
        if na_values is not None:
            return replace_na_values_pandas(df, columns_used, na_values)
        return df  # No change

    def _get_records_evaluated_pandas(self, df: pd.DataFrame) -> int:
        """
        Computes the number of records that are evaluated against the rule.

        By default, counts non-null entries in the target field. Override this
        for rules involving multiple columns or different completeness logic.

        Args:
            df (pd.DataFrame): The DataFrame to process.

        Returns:
            int: The count of records in the field being assessed.
        """

        records_evaluated_mask = self._get_records_evaluated_mask_pandas(df)

        return records_evaluated_mask.sum()

    def _get_records_passing_pandas(self, df: pd.DataFrame) -> int:
        """
        Abstract method to compute the number of records passing the data quality rule.

        This must be customised for each specific rule.

        Args:
            df (pd.DataFrame): The DataFrame to process.

        Returns:
            int: The count of records passing the rule's criteria.
        """
        evaluated_mask = self._get_records_evaluated_mask_pandas(df)
        passing_mask = self._get_records_passing_mask_pandas(df)
        passing_mask = self._replace_na_in_bool_mask(passing_mask)
        return (evaluated_mask & passing_mask).sum()

    def _require_failed_records_sample(self, pass_rate: float | None) -> bool:
        """
        Determines whether a diagnostic sample of failed records should be collected.

        Args:
            pass_rate (float | None): The rule pass rate, or None if no records were evaluated.

        Returns:
            bool: True if failed record samples are required; otherwise, False.
        """

        if pass_rate is None:
            # on the occasion there are no records to check, the pass rate will be None
            return False
        elif pass_rate == 1.0:
            # there are no failed records for a perfect score
            return False
        else:
            return True

    def _get_records_failed_mask_pandas(self, df: pd.DataFrame) -> pd.Series:
        """
        Abstract method to generate a boolean mask for records failing the rule.

        Args:
            df (pd.DataFrame): The DataFrame to process.

        Returns:
            pd.Series: Boolean mask where True indicates a failing record.
        """

        passing_mask = self._get_records_passing_mask_pandas(df)
        passing_mask = self._replace_na_in_bool_mask(passing_mask)
        evaluated_mask = self._get_records_evaluated_mask_pandas(df)
        return evaluated_mask & ~passing_mask

    def _replace_na_in_bool_mask(self, mask: pd.Series) -> pd.Series:
        """If we get 'None' in a boolean mask we can't conduct mask operations such as
        inverting it or logical AND / OR, this replaces None / NA, with False.

        This method can be overridden by child classes"""

        return mask.fillna(False)

    def _get_records_failed_pandas(
        self,
        df: pd.DataFrame,
    ) -> list[dict]:
        """
        Returns a list of unique records from the field that failed the rule.

        Args:
            df (pd.DataFrame): The DataFrame instance to process
                (assumes df has been filtered to just contain required columns).

        Returns:
            list: Unique records from the field corresponding to failed records.
                In format [{colA : valueA, colB : valueB}, {...etc}]
        """

        records_failed_mask = self._get_records_failed_mask_pandas(df)
        return df.loc[records_failed_mask, :].drop_duplicates().to_dict("records")

    # ------------ Spark Evaluation ----------------

    def _evaluate_in_spark(self, spark_df: SparkDataFrame) -> DataQualityResult:
        """By default we execute everything in pandas via mapInPandas,
        this partitions the data automatically and sends dataframes to each Spark worker,
        we then aggregate the resulting data."""
        from gchq_data_quality.spark.dataframe_operations import flatten_spark
        from gchq_data_quality.spark.models import DataQualityResultSparkSchema
        from gchq_data_quality.spark.utils.results_utils import (
            aggregate_spark_df_to_data_quality_result,
        )

        columns_used = self._get_columns_used_pandas()
        flattened_df = flatten_spark(spark_df, columns_used)

        spark_safe_rule = self._get_spark_safe_rule()
        result_schema = DataQualityResultSparkSchema.get_schema()

        def pandas_mapper(
            pdf_iter: Iterator[pd.DataFrame],
        ) -> Iterator[pd.DataFrame]:  # pragma: no cover
            """Lightweight wrapper for a function that will work within mapInPandas"""
            for pdf in pdf_iter:
                yield spark_safe_rule._evaluate_in_pandas_output_dataframe(pdf)

        # Spark's mapInPandas has to return a spark dataframe
        # to maintain return consistency across different self.evaluate() functions, we pass the dataframe back into a DataQualityResult
        # schema (the spark result_schema is directly equivalent to a DataQualityResult object)
        result_spark_df = flattened_df.mapInPandas(pandas_mapper, schema=result_schema)  # type: ignore - for some reason linter does not like Iterator[pd.DataFrame] in pandas_mapper
        return aggregate_spark_df_to_data_quality_result(result_spark_df)

    # ---------- Spark Helpers ---------------
    def _get_spark_safe_rule(self) -> Self:
        """Returns a modified (deep copy) of the rule with spark safe column names
        in any column used to evaluate the rule.
        This is required when working with nested data, as if we want to measure 'customers.age'
        after we flatten the dataframe and exract the age property from the 'customers' object
        our column will be renamed to customers_age when it gets passed to _evaluate_in_pandas.

        As 'customers.age' is not a valid Spark column name once the data is flattened.

        This is overridden for each subrule type if more than self.field is used"""
        from gchq_data_quality.spark.utils.rules_utils import (
            get_spark_safe_column_name,
        )

        rule_copy = self.model_copy()
        rule_copy.field = get_spark_safe_column_name(self.field)
        return rule_copy

    def _evaluate_in_pandas_output_dataframe(
        self, df: pd.DataFrame
    ) -> pd.DataFrame:  # pragma: no cover
        """Wrapper to ensure when executing in Spark we return a DataFrame
        (this is a Spark requirement), yet we want to maintain the behaviour that _evaluate_in_pandas
        returns a DataQualityResult (so did not want to override that).

        Returns:
            A Dataframe in a format that matches SparkDataQualityResultSchema"""
        dq_result = self._evaluate_in_pandas(df)

        # We can't return a DataQualityResult object in Spark, it has to be a pandas dataframe
        return dq_result._to_spark_schema_df()

    # ---------- Elasticsearch ---------------

    def _evaluate_in_elastic(
        self, es: Elasticsearch, index_name: str, query: dict | None = None
    ) -> DataQualityResult:
        if query is None:
            query = {"query": {"match_all": {}}}
        """Not yet implemented"""

        raise NotImplementedError(
            "Elasticsearch querying not yet implemented - just dataframes"
        )
