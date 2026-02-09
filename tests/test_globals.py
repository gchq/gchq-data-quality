# (c) Crown Copyright GCHQ \n
import pandas as pd

from gchq_data_quality.globals import SampleConfig
from gchq_data_quality.rules.uniqueness import UniquenessRule


def test_changing_global_records_failed_sample_size() -> None:
    # Prepare DataFrame with duplicates
    df = pd.DataFrame({"col": [1, 2, 2, 3, 4, 4, 4, 5, 5]})

    # Set to only sample 1 duplicate value and include 1 invalid row number
    SampleConfig.RECORDS_FAILED_SAMPLE_SIZE = 1
    result = UniquenessRule(field="col").evaluate(df)

    assert result.records_failed_sample and len(result.records_failed_sample) == 1
    assert result.records_failed_ids and len(result.records_failed_ids) == 1

    SampleConfig.RECORDS_FAILED_SAMPLE_SIZE = 2

    result = UniquenessRule(field="col").evaluate(df)

    assert result.records_failed_sample and len(result.records_failed_sample) == 2
    assert result.records_failed_ids and len(result.records_failed_ids) == 2

    # set back to defaults
    SampleConfig.RECORDS_FAILED_SAMPLE_SIZE = 10
