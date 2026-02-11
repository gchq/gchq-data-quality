# Data Quality Tutorial - Python 2

## Prerequisites

- Completion of the [**Python 1 Tutorial**](python-basic.md) (comfortable running data quality functions on DataFrames).

> **Coding:**  
You should know about pandas DataFrames and basic Python syntax.

## Aim

- Create Data Quality config files (YAML-based rule lists).
- Run these configs directly against your DataFrames.
- Manage your regex patterns from a single YAML file


## 1. Reusing Example Data

```python
import pandas as pd
from datetime import datetime

df = pd.DataFrame({
    "id": [1, 2, 3, 3, 5],
    "name": ["John", "Jane", "Dave", None, "Missing"],
    "age": [30, 25, 102, 15, -5],
    "email": [
        "john@example.com",
        "jane@example.com",
        "dave@example",
        "test@test.com",
        "alice@example.com",
    ],
    "category": ["A", "B", "C", "D", "X"],
    "score": [10, 20, 30, 40, -1],
    "date": [
        datetime(2023, 1, 1),
        datetime(2023, 2, 1),
        datetime(2023, 3, 1),
        datetime(2021, 1, 1),
        datetime(2023, 5, 1),
    ]
})
```

## 2. YAML Config Files for Data Quality Rules

### Why YAML?
YAML is human-readable and machine-readable, well easier for a human to read than JSON anyway.

### Key structure:
- Overall metadata (e.g. dataset_name, measurement_time)
    **all optional**
- List of rules
    **you must have at least one rule** before you run the config.

### Example:

```yaml
dataset_name: My Source Data
measurement_sample: 10% of records
lifecycle_stage: null
rules:
  - field: id
    function: uniqueness
  - field: name
    na_values: ''
    function: validity_regex
    regex_pattern: '[A-z0-9_]'
```

#### Lists in YAML

```yaml
valid_values: [A, B, C, D]   # simple
valid_values:
  - A
  - B
  - C
  - D               # verbose, useful for long lists
```

#### Regex in YAML

Always surround `regex_pattern` with **single quotes**:

```yaml
regex_pattern: '[A-Za-z]+'
regex_pattern: '\d{4}-\d{2}-\d{2}'
regex_pattern: 'don''t'    # To include a single quote
```

## 3. Loading and Validating Config Files

```python
from gchq_data_quality import DataQualityConfig

config = DataQualityConfig.from_yaml("your_config.yaml")
```

You can load multiple files. 

We find it makes sense to split your rules into separate files based on what you are measuring. For example, if you own a pet shop and have rules around pet details and customer details and orders, you might want a `dates.yaml` for storing rules relating to all your dates and `names.yaml` relating to rules for names of pets and owners:

```python
config = DataQualityConfig.from_yaml(['dates.yaml', 'names.yaml'])
```

## 4. Running a Config Against Your Data

Once your config is loaded:

```python
report = config.execute(df)
print(report.to_dataframe(measurement_time_format="%Y-%m-%d %H:%M"))
```

You can adjust config metadata programmatically. It can be useful to override `measurement_time`, as you may want to pretend the data was measured at the date of ingest, rather than when you actually measured it. It can help make sense of your analysis later to understand the quality of the data based on what it landed.

```python
from datetime import timezone

config.measurement_sample = "Test Sample"
config.dataset_name = "Overwrite Dataset Name"
config.measurement_time = datetime.now(tz=timezone.utc)
```

## 5. Creating a Config File From a Report

A typical workflow:
- Experiment with rules in Python
- Produce a DataQualityReport
- Extract those rules back into a deployable YAML config and modify
  - saves you writing out the entire YAML file from scratch

```python
config_from_report = DataQualityConfig.from_report(report)
config_from_report.to_yaml("yaml_from_report.yaml", overwrite=True)
```

## 6. Mangaing Regular Expressions

Use a separate YAML file for regex patterns, to keep config rules readable and maintainable.

`regex_patterns.yaml`:
```yaml
EMAIL_REGEX: '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
PHONE_REGEX: '[0-9]+'
```

Reference pattern names instead of raw regex in your rules:
```yaml
- field: email
  function: validity_regex
  regex_pattern: EMAIL_REGEX
```

When loading your config:

```python
config = DataQualityConfig.from_yaml(
    "config_with_regex_refs.yaml", 
    regex_yaml_path="regex_patterns.yaml"
)
```

## 7. Tweak Output Display (Advanced)

Control sample output size globally:

```python
from gchq_data_quality.globals import SampleConfig
SampleConfig.RECORDS_FAILED_SAMPLE_SIZE = 25
```