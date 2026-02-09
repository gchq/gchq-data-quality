# (c) Crown Copyright GCHQ \n
"""
Provides utility functions for data quality processing in Spark pipelines.

Specifically,
    - Aggregating partitioned data quality results to a single DataQualityReport


"""

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame

from gchq_data_quality.errors import DQFunctionError
from gchq_data_quality.results.models import DataQualityReport, DataQualityResult


def _spark_df_to_pandas(spark_df: SparkDataFrame) -> pd.DataFrame:
    """Separating this func in case we need to use an alternative to toPandas() at a later date (which uses pyarrow)"""
    return spark_df.toPandas()


def aggregate_spark_df_to_data_quality_result(
    spark_df: SparkDataFrame,
) -> DataQualityResult:
    """
    Aggregates partitioned Spark data quality results and combines them into a single DataQualityResult.

    Args:
        spark_df: Spark DataFrame containing one or more partitioned data quality results (e.g. per rule or partition).

    Returns:
        DataQualityResult: A validated, aggregated single DataQualityResult instance representing all input partitions.

    Raises:
        DQFunctionError: If there is no data to aggregate from the Spark DataFrame.

    Example:
        ```python
        >>> dq_result = aggregate_spark_df_to_data_quality_result(spark_df)
        >>> print(dq_result.summary)
        ```
    """

    # These dataframe are the spark schema serialised versions (so records_failed lists are json strings.)

    many_pdf = _spark_df_to_pandas(
        spark_df
    )  # multiple DQ Results per rule against each partition
    temp_report = DataQualityReport.from_dataframe(many_pdf)
    single_pdf = temp_report._aggregate_results()

    dq_result_data = single_pdf.to_dict(orient="records")
    if dq_result_data:
        dq_result = DataQualityResult.model_validate(dq_result_data[0])
        dq_result.records_failed_ids = None  # we overwrite this as unreliable in Spark
        return dq_result
    else:
        raise DQFunctionError(
            f"No data returned from Spark. Aggregated dfs size: {many_pdf.shape}"
        )
