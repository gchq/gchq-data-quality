# (c) Crown Copyright GCHQ \n
import pandas as pd
import pytest

from gchq_data_quality.globals import SampleConfig
from gchq_data_quality.results.models import DataQualityReport
from gchq_data_quality.results.utils import (
    aggregate_records_failed_samples,
    records_failed_ids_are_int,
    shift_records_failed_ids,
)


def test_aggregate_records_failed_samples(
    aggregate_records_failed_samples_case: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    inputs = aggregate_records_failed_samples_case["inputs"]
    expected = aggregate_records_failed_samples_case["expected"]

    # Set limit if included
    limit = inputs.get("limit", 3)
    monkeypatch.setattr(SampleConfig, "RECORDS_FAILED_SAMPLE_SIZE", limit)

    result = aggregate_records_failed_samples(
        pd.Series(inputs["records_failed_sample_series"])
    )
    assert result == expected["records_failed_sample"]


def test_aggregate_partition_dq_results_defaults() -> None:
    # Simulate minimal DQ result rows across Spark partitions
    df = pd.DataFrame(
        [
            {
                "dataset_name": "dataset1",
                "measurement_sample": "all",
                "lifecycle_stage": None,
                "measurement_time": "2020-01-01T00:00:00.000Z",
                "dataset_id": None,
                "field": "email",
                "data_quality_dimension": "Validity",
                "rule_id": None,
                "rule_description": '{"regex_pattern": ".+@.+"}',
                "rule_data": "[rule_info]",
                "records_failed_sample": [{"id": 1}, {"id": 2}],
                "records_failed_ids": [0, 1],
                "records_evaluated": 10,
                "pass_rate": 0.9,
                # Simulate indexing Spark partition
            },
            {
                "dataset_name": "dataset1",
                "measurement_sample": "all",
                "lifecycle_stage": None,
                "measurement_time": "2020-01-01T00:00:05.000Z",
                "dataset_id": None,
                "field": "email",
                "data_quality_dimension": "Validity",
                "rule_id": None,
                "rule_description": '{"regex_pattern": ".+@.+"}',
                "rule_data": "[rule_info]",
                "records_failed_sample": [{"id": 3}, {"id": 4}],
                "records_failed_ids": [2, 3],
                "records_evaluated": 5,
                "pass_rate": 1.0,
            },
        ]
    )

    # Run aggregation
    report = DataQualityReport.from_dataframe(df)
    result = report._aggregate_results()

    # Assert shape and defaults
    assert result.shape[0] == 1  # should aggregate into one row
    row = result.iloc[0]

    # Check defaults (you can tweak as needed for your model)
    assert row["dataset_id"] is None, "dataset_id should be None"
    assert len(row["records_failed_sample"]) == 4
    assert row["pass_rate"] == pytest.approx((10 * 0.9 + 5 * 1.0) / (10 + 5), 1e-4)
    # records_failed_ids shouldn't exist post-agg, or should be None/empty
    assert row["records_failed_ids"] == [0, 1, 2, 3]


def test_records_failed_ids_are_int(records_failed_ids_are_int_case: dict) -> None:
    """Test records_failed_ids_are_int with example cases."""
    inputs = records_failed_ids_are_int_case["inputs"]
    expected = records_failed_ids_are_int_case["expected"]["is_all_int"]
    result = records_failed_ids_are_int(**inputs)
    assert result == expected


def test_shift_records_failed_ids(shift_records_failed_ids_case: dict) -> None:
    """Test shift_records_failed_ids with example cases."""
    inputs = shift_records_failed_ids_case["inputs"]
    expected = shift_records_failed_ids_case["expected"]["shifted_row_numbers"]
    result = shift_records_failed_ids(**inputs)
    assert result == expected
