# API Reference

## Rules

::: gchq_data_quality.rules.uniqueness.UniquenessRule
    options:
        members: false
        show_root_heading: true
        heading_level: 3

::: gchq_data_quality.rules.completeness.CompletenessRule
    options:
        members: false
        show_root_heading: true
        heading_level: 3

::: gchq_data_quality.rules.accuracy.AccuracyRule
    options:
        members: false
        show_root_heading: true
        heading_level: 3

::: gchq_data_quality.rules.consistency.ConsistencyRule
    options:
        members: false
        show_root_heading: true
        heading_level: 3

::: gchq_data_quality.rules.timeliness.TimelinessRelativeRule
    options:
        members: false
        show_root_heading: true
        heading_level: 3

::: gchq_data_quality.rules.timeliness.TimelinessStaticRule
    options:
        members: false
        show_root_heading: true
        heading_level: 3

::: gchq_data_quality.rules.validity.ValidityNumericalRangeRule
    options:
        members: false
        show_root_heading: true
        heading_level: 3

::: gchq_data_quality.rules.validity.ValidityRegexRule
    options:
        members: false
        show_root_heading: true
        heading_level: 3

## Data Quality Configuration and Results

::: gchq_data_quality.config.DataQualityConfig
    options:
        members: false
        show_root_heading: true
        heading_level: 3

::: gchq_data_quality.results.models.DataQualityReport
    options:
        members: false
        show_root_heading: true
        heading_level: 3

::: gchq_data_quality.results.models.DataQualityResult
    options:
        members: false
        show_root_heading: true
        heading_level: 3

## Spark Utilities

::: gchq_data_quality.spark.dataframe_operations.flatten_spark
    options:
        members: false
        show_root_heading: true
        heading_level: 3

## Types and Base Rule

The way we categorise the data quality dimensions

::: gchq_data_quality.models.DamaFramework
    options:
        members: false
        show_root_heading: true
        heading_level: 3

The base rule is never called by a user, but serves as a parent for all data quality rules.
::: gchq_data_quality.rules.base.BaseRule
    options:
        members: true
        show_root_heading: true
        heading_level: 3