# (c) Crown Copyright GCHQ \n
import pytest
from elasticsearch import Elasticsearch

from gchq_data_quality.models import DamaFramework
from gchq_data_quality.rules import CompletenessRule


def test_evaluate_in_elastic_raises_from_evaluate() -> None:
    rule = CompletenessRule(
        field="id", data_quality_dimension=DamaFramework.Completeness
    )
    es_client = Elasticsearch(
        hosts=["http://localhost:9600"]
    )  # This will not perform a real connection

    # The evaluate() will dispatch to evaluate_in_elastic and should raise NotImplementedError
    with pytest.raises(NotImplementedError) as excinfo:
        rule.evaluate(es_client, "some_index")
    assert "Elasticsearch querying not yet implemented" in str(excinfo.value)
