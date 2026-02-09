# (c) Crown Copyright GCHQ \n
from unittest.mock import Mock

import pytest

from gchq_data_quality.models import DamaFramework
from gchq_data_quality.rules import (
    CompletenessRule,
)


def test_evaluate_raises_from_bad_input() -> None:
    rule = CompletenessRule(
        field="id", data_quality_dimension=DamaFramework.Completeness
    )

    data = Mock()
    # The evaluate() will dispatch to evaluate_in_elastic and should raise NotImplementedError
    with pytest.raises(ValueError) as excinfo:
        rule.evaluate(data)
    assert "You must pass in" in str(excinfo.value)


def test_output_formats() -> None:
    rule = CompletenessRule(
        field="id", data_quality_dimension=DamaFramework.Completeness
    )

    rule_dict = rule.to_dict()
    rebuilt_rule_dict = CompletenessRule.model_validate(rule_dict)
    assert isinstance(rule_dict, dict)
    assert rebuilt_rule_dict == rule

    rule_json = rule.to_json()
    assert isinstance(rule_json, str)
    rebuilt_rule = CompletenessRule.model_validate_json(rule_json)
    assert rebuilt_rule == rule
