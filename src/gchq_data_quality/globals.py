# (c) Crown Copyright GCHQ \n
"""
Global configuration for gchq_data_quality.

Sets the maximum number of failed records to include in sample outputs
(`records_failed_sample` and `records_failed_ids` fields of DataQualityResult).

Configuration:
    SampleConfig.RECORDS_FAILED_SAMPLE_SIZE (int):
        Maximum number of failed records to sample. Default is 10.
        Adjust as needed before rule evaluation.

Example:
    ```python
    from gchq_data_quality.globals import SampleConfig

    # Limit output samples to 5 records
    SampleConfig.RECORDS_FAILED_SAMPLE_SIZE = 5
    ```

Notes:
    No instantiation is required—access class variables directly.

"""


class SampleConfig:
    """
    Configuration for controlling data quality result sampling.

    Attributes:
        RECORDS_FAILED_SAMPLE_SIZE (int):
            Maximum number of failed records to include in sample outputs
            such as records_failed_sample and records_failed_ids.
            Adjust this value before running data quality checks.
    """

    RECORDS_FAILED_SAMPLE_SIZE: int = 10
