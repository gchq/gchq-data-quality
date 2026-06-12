# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)

## [Unreleased]

### Added

-

### Fixed

-

### Changed

-

### Removed

-

### Deprecated

-

## [1.2.1] - 2026-06-12

### Removed
- Removed use of Pydantic's experimental `MISSING` sentinel for `records_passed` in `DataQualityResult`. Relying on experimental Pydantic internals is fragile and may break across versions; `records_passed` now defaults to `None`.

> **Note:** `1.2.0` has been yanked on PyPI due to this issue. Users should upgrade directly to `1.2.1`.

## [1.2.0] - 2026-06-09

### Added
- `DataQualityReport.__add__` and `__len__` support: reports can now be combined with `+`/`+=`, and `len(report)` returns the number of results.
- `records_passed` is now included in the standard rule evaluation output (`DataQualityResult`), enabling weighted pass rate calculations in dashboards (e.g. weighting by `records_evaluated` rather than a simple average of `pass_rate`).

### Changed
- Rule classes have been renamed to more descriptive `Values*` names (old names remain importable as aliases for backward compatibility):
  - `UniquenessRule` → `ValuesAreUnique`
  - `CompletenessRule` → `ValuesAreComplete`
  - `AccuracyRule` → `ValuesMatchList`
  - `ConsistencyRule` → `ValuesMatchExpression`
  - `ValidityRegexRule` → `ValuesMatchRegex`
  - `ValidityNumericalRangeRule` → `ValuesMatchNumericalRange`
  - `TimelinessStaticRule` → `ValuesMatchStaticTimeBounds`
  - `TimelinessRelativeRule` → `ValuesMatchRelativeTimeBounds`
- Improved error feedback when YAML-defined regex patterns are malformed, with actionable messages to help users diagnose and fix the issue.

### Removed
- Removed the skeleton Elasticsearch implementation. Because query results are highly dependent on index mappings and cross-column comparisons are awkward, the recommended approach is to use `eland` to sample data from Elasticsearch into a DataFrame and apply rules there.

## [1.1.0] - 2026-02-23

### Changed
- Extended pandas version support to include pandas 3.0. This required updates to the test suite but does not change runtime behaviour.


## [1.0.0]

### Added

- Note here

### Fixed

- Note here

### Changed

- Note here

### Removed

- Note here


[//]: # (## [M.m.p] - YYYY-mm-dd)

[//]: # (### Added)
[//]: # (This is where features that have been added should be noted.)

[//]: # (### Fixed)
[//]: # (This is where fixes should be noted.)

[//]: # (### Changed)
[//]: # (This is where changes from previous versions should be noted.)

[//]: # (### Removed)
[//]: # (This is where elements which have been removed should be noted.)

[//]: # (### Deprecated)
[//]: # (This is where existing but deprecated elements should be noted.)

[1.2.1]: https://github.com/gchq/gchq-data-quality/releases/tag/v1.2.1
[1.2.0]: https://github.com/gchq/gchq-data-quality/releases/tag/v1.2.0
[1.1.0]: https://github.com/gchq/gchq-data-quality/releases/tag/v1.1.0
[1.0.0]: https://github.com/gchq/gchq-data-quality/releases/tag/v1.0.0

[beartype]: https://pypi.org/project/beartype/
[pre-commit]: https://pre-commit.com/


