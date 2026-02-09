# (c) Crown Copyright GCHQ \n
"""Classes or pydantic models required when working with data quality rules in Spark"""

from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


class DataQualityResultSparkSchema:
    """Spark requires a schema when returning a dataframe using MapInPandas.

    This schema should *exactly* match the output format of the pandas_mapper function in BaseRule,
    where we run MapInPandas in Spark and output a dataframe.
    Note that due to strict typing requirements in Spark, we string serialise records_failed_* fields, as we don't know
    ahead of time what types may be present in any output sample. These are json.loads() later on to get back to a list[dict]

    See BaseRule for implementation.
    """

    schema = StructType(
        [
            StructField("dataset_name", StringType(), True),
            StructField("measurement_sample", StringType(), True),
            StructField("lifecycle_stage", StringType(), True),
            StructField("measurement_time", StringType(), False),
            StructField("dataset_id", StringType(), True),
            StructField("field", StringType(), False),
            StructField("data_quality_dimension", StringType(), False),
            StructField("rule_id", StringType(), True),
            StructField("rule_description", StringType(), True),
            StructField("rule_data", StringType(), True),
            StructField(
                "records_failed_ids", StringType(), True
            ),  # in report.to_dataframe() we turn integer indexes to strings
            StructField("records_failed_sample", StringType(), True),
            StructField("records_evaluated", IntegerType(), True),
            StructField("pass_rate", FloatType(), True),
        ]
    )

    @classmethod
    def get_schema(cls) -> StructType:
        return cls.schema
