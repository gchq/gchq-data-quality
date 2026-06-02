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
masks are typically the same for every rule type and are specified in BaseRule.

Preferred (new) rule class names - these are in __all__:
    - ValuesAreUnique
    - ValuesMatchList
    - ValuesAreComplete
    - ValuesMatchRegex
    - ValuesMatchNumericalRange
    - ValuesMatchExpression
    - ValuesMatchStaticTimeBounds
    - ValuesMatchRelativeTimeBounds

Legacy rule class names (importable but not in __all__):
    - UniquenessRule
    - AccuracyRule
    - CompletenessRule
    - ConsistencyRule
    - TimelinessRelativeRule
    - TimelinessStaticRule
    - ValidityNumericalRangeRule
    - ValidityRegexRule

Whilst the user can call these rules and evaluate them against a dataframe

ValuesAreUnique.evaluate(df)

The intention of the package is that multiple rules are wrapped up into a DataQualityConfig class
and executed together against a dataframe.

DataQualityConfig(rules=my_rules_list).execute(df)
"""

# New preferred names (in __all__)
from gchq_data_quality.rules.uniqueness import ValuesAreUnique  # noqa
from gchq_data_quality.rules.accuracy import ValuesMatchList  # noqa
from gchq_data_quality.rules.completeness import ValuesAreComplete  # noqa
from gchq_data_quality.rules.validity import ValuesMatchRegex, ValuesMatchNumericalRange  # noqa
from gchq_data_quality.rules.consistency import ValuesMatchExpression  # noqa
from gchq_data_quality.rules.timeliness import (  # noqa
    ValuesMatchStaticTimeBounds,
    ValuesMatchRelativeTimeBounds,
)

# Legacy names (importable for backward compatibility, not in __all__)
from gchq_data_quality.rules.uniqueness import UniquenessRule  # noqa
from gchq_data_quality.rules.accuracy import AccuracyRule  # noqa
from gchq_data_quality.rules.completeness import CompletenessRule  # noqa
from gchq_data_quality.rules.consistency import ConsistencyRule  # noqa
from gchq_data_quality.rules.timeliness import (  # noqa
    TimelinessRelativeRule,
    TimelinessStaticRule,
)
from gchq_data_quality.rules.validity import (  # noqa
    ValidityNumericalRangeRule,
    ValidityRegexRule,
)

from gchq_data_quality.config import DataQualityConfig  # noqa
from gchq_data_quality.results.models import DataQualityReport  # noqa

__all__ = [
    # New preferred names
    "ValuesAreUnique",
    "ValuesMatchList",
    "ValuesAreComplete",
    "ValuesMatchRegex",
    "ValuesMatchNumericalRange",
    "ValuesMatchExpression",
    "ValuesMatchStaticTimeBounds",
    "ValuesMatchRelativeTimeBounds",
    # Config & report
    "DataQualityConfig",
    "DataQualityReport",
]
