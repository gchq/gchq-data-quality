# (c) Crown Copyright GCHQ \n
"""
Data quality rule definitions for the gchq_data_quality framework.

This module provides data quality rules for the 6 DAMA Dimensions of Data Quality:
- Uniqueness
- Accuracy
- Completeness
- Validity
- Consistency
- Timeliness

They inherit from a core BaseRule class. All data quality evaluation is built on a consistent method:
1. Determine the records that are evaluated (as a boolean mask) - records_evaluated_mask
    The total records evaluated here is then the sum of the mask.
2. Determine the records that pass the rule (as a boolean mask) - records_passing_mask
    The count of records_passing is the sum of records_passing_mask AND records_evaluated_mask
    (for various reasons you can have records passing a rule that are not in the evaluation set, e.g. they are NULL)

    The pass_rate is then records_passing / records_evaluated


You can see the mechanisms in each rule primarily by looking at the masks that are created. The metrics derived from these
masks are the same for every rule type and are specified in BaseRule. i.e. once we work out what rules are passing vs evaluate
all follow-on logic is the same.

Available rule classes:
    - UniquenessRule
    - AccuracyRule
    - CompletenessRule
    - ConsistencyRule
    - TimelinessRelativeRule
    - TimelinessStaticRule
    - ValidityNumericalRangeRule
    - ValidityRegexRule

Whilst the user can call these rules and evaluate them against a dataframe

UniquenessRule.evalute(df)

The intention of the package is that multiple rules are wrapped up into a DataQualityConfig class
and executed together against a dataframe.

DataQualityConfig(rules=my_rules_list).execute(df)
"""

from gchq_data_quality.rules.uniqueness import UniquenessRule  # noqa
from gchq_data_quality.rules.accuracy import AccuracyRule  # noqa
from gchq_data_quality.rules.completeness import CompletenessRule  # noqa
from gchq_data_quality.rules.consistency import ConsistencyRule  # noqa
from gchq_data_quality.rules.timeliness import (
    TimelinessRelativeRule,
    TimelinessStaticRule,
)  # noqa
from gchq_data_quality.rules.validity import (
    ValidityNumericalRangeRule,
    ValidityRegexRule,
)  # noqa

__all__ = [
    "UniquenessRule",
    "AccuracyRule",
    "CompletenessRule",
    "ConsistencyRule",
    "TimelinessRelativeRule",
    "TimelinessStaticRule",
    "ValidityNumericalRangeRule",
    "ValidityRegexRule",
]
