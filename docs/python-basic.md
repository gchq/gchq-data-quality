# Data Quality Tutorial - Python 1

## Prerequisites
- Familiarity with pandas DataFrames.
- Have read [What is Data Quality](what-is-data-quality.md)

**You should therefore already know:**

- What the 6 DAMA data quality dimensions are.
- What the pass rate of a completeness rule means.

## Aim
- Run all 8 data quality rules on a DataFrame.
- Export results to CSV.

## 1. Create Sample DataFrame

We'll use a purposely "messy" dataset to demonstrate data quality rules. Perhaps this is a list of members for your local sports club.

```python
import pandas as pd
from datetime import datetime, timedelta

df = pd.DataFrame({
    "id": [1, 2, 3, 3, 5],  # Duplicate ID
    "name": ["John", "Jane", "Dave", None, "Missing"],  # Missing value
    "age": [30, 25, 102, 15, -5],  # Implausible negative age
    "email": [
        "john@example.com",
        "jane@example.com",
        "dave@example",
        "test@test.com",
        "alice@example.com",
    ],  # Malformed email
    "category": ["A", "B", "C", "D", "X"],  # 'X' not allowed
    "score": [10, 20, 30, 40, -1],  # -1 as a missing value marker
    "date": [
        datetime(2023, 1, 1),
        datetime(2023, 2, 1),
        datetime(2023, 3, 1),
        datetime(2021, 1, 1),  # Outside of recent range
        datetime(2023, 5, 1),
    ]
})
df.head()
```



## 2. Apply Data Quality Rules

We’ll use the package’s data quality rules and report classes. Import them:

```python
from gchq_data_quality import (
    AccuracyRule,
    CompletenessRule,
    ConsistencyRule,
    TimelinessRelativeRule,
    TimelinessStaticRule,
    UniquenessRule,
    ValidityNumericalRangeRule,
    ValidityRegexRule,

    DataQualityConfig,
    DataQualityReport,
)

# or

from gchq_data_quality import * # this will just import those functions above
```

Initialise a report to collect the results:

```python
FINAL_REPORT = DataQualityReport()
```

### 2.1 UniquenessRule

Checks for duplicate values in a column.

```python
uniqueness_rule = UniquenessRule(field="id")
dq_result = uniqueness_rule.evaluate(df)
print(f"Uniqueness pass rate: {dq_result.pass_rate}")
print(f"Rows with duplicate ids: {dq_result.records_failed_ids}")
```

Add to report:
```python
FINAL_REPORT.results.append(dq_result)
```

### 2.2 CompletenessRule

Checks for missing/incomplete values.

```python
completeness_rule = CompletenessRule(field="name")
completeness_result = completeness_rule.evaluate(df)
print(f"Completeness pass rate: {completeness_result.pass_rate}")
```

To treat `"Missing"` as a null/missing value:
```python
completeness_rule.na_values = ["Missing"]
completeness_result = completeness_rule.evaluate(df)
print(f"With 'Missing' as null: {completeness_result.pass_rate}")
```
Add to report:
```python
FINAL_REPORT.results.append(completeness_result)
```

### 2.3 AccuracyRule

Checks if values come from a list of valid entries. Useful for known values like ISO Country Codes.
Or perhaps in our Sports Club example, the sports people play e.g. `['Football', 'Hockey']` - this ensures consistent spelling and captilisation is checked in your data

```python
accuracy_rule = AccuracyRule(field="category", valid_values=["A", "B", "C", "D"])
accuracy_result = accuracy_rule.evaluate(df)
print(f"Accuracy pass rate: {accuracy_result.pass_rate}")
print(f"Values not in allowed list: {accuracy_result.records_failed_sample}")
```

**Inverse check:** to flag presence of forbidden values.
```python
accuracy_rule.inverse = True
forbidden_result = accuracy_rule.evaluate(df)
print(f"Presence of forbidden values: {forbidden_result.records_failed_sample}")
```
Add initial result to the report:
```python
FINAL_REPORT.results.append(accuracy_result)
```

### 2.4 ConsistencyRule

Checks logical relationships (often between columns).

> **Expression Syntax:** Use backticks around **all** column names in your expressions.

You can experiment with expressions directly in a pandas dataframe using: `df.eval("<your expression here>")`

```python
# Simple numeric rule
consistency_rule = ConsistencyRule(field="age", expression="`age` > 3")
consistency_result = consistency_rule.evaluate(df)
print(f"Consistency pass rate: {consistency_result.pass_rate}")

# Compound rule: if-then
# This rule will then ONLY be evaluated against rows that match the 'if' statement, skipping others
consistency_rule2 = ConsistencyRule(
    field="age", expression={"if": "`age` > 3", "then": "`score` <= 40"}
)
consistency_result2 = consistency_rule2.evaluate(df)
print(f"Constrained pass rate: {consistency_result2.pass_rate}")
```
Add to report:
```python
FINAL_REPORT.results.append(consistency_result)
FINAL_REPORT.results.append(consistency_result2)
```

#### Handling Nulls

`skip_if_null` can be `"any"` (default), `"all"`, or `"never"`.

```python
consistency_rule.skip_if_null = "never"
never_skip_result = consistency_rule.evaluate(df)
print(f"Pass rate with nulls included: {never_skip_result.pass_rate}")
```


### 2.5 Timeliness Rules

Check if dates fall inside certain ranges (inclusively). If no time-zone is provided it will assume UTC.

You can pass in strings or datetime objects.

### 2.5A TimelinessStaticRule

```python
timeliness_static_rule = TimelinessStaticRule(
    field="date", start_date="2023-01-01", end_date=datetime(2023, 6, 2)
)
timeliness_static_result = timeliness_static_rule.evaluate(df)
print(f"Timeliness (static) pass rate: {timeliness_static_result.pass_rate}")
FINAL_REPORT.results.append(timeliness_static_result)
```

**Note**: if you provide no times, then it assumes 00:00hrs, so take care when thinking about your date boundaries, as pretty much all datetime values on 1st Jan 2023 will be later than 00:00hrs!

The above example will check that the 'date' field occurs sometime on 2023-01-01 00:00:00 -> 2023-01-01 23:59:59

### 2.5B TimelinessRelativeRule

The **TimelinessRelativeRule** checks whether a date column falls within a time window defined **relative to another date**. This is useful when you need to validate dates that depend on a reference, such as ensuring bookings occur within N days of the order date.

#### Options

- **reference_date**:
    - String date (`"2023-01-01"`), a `datetime` object, or `'now'` (UTC now).
- **reference_column**:
    - Only use if `reference_date` is left blank.
    - Compares each row’s field against a date in another column (e.g. `delivery_date` versus `order_date`). i.e. the `reference_date` for each 
    record comes from another column in the same row in the dataset.
- **start_timedelta** and **end_timedelta**:
    - Allowed time window before/after reference.
    - Such that your start boundary is `reference_date + start_timedelta`, and end boundary is `reference_date + end_timedelta`.
    - So a negative `start_timedelta` is needed to set a boundary before the `reference date`.
    - Accepts `timedelta(days=5)`, `'5d'`, `'P5D'`, `'+6h'`, etc.
    - `'0d'` or `timedelta(0)` means your boundary is equal to the `reference_date` (or corresponding value in the `reference_column`)



#### Examples

**1. Date must be within 2 years *after* reference date:**
```python
from datetime import timedelta
timeliness_relative_rule = TimelinessRelativeRule(
    field="date",
    reference_date="2023-01-01",
    start_timedelta=0,
    end_timedelta=timedelta(days=365 * 2),
)
timeliness_relative_result = timeliness_relative_rule.evaluate(df)
print(f"Timeliness (relative) pass rate: {timeliness_relative_result.pass_rate}")
FINAL_REPORT.results.append(timeliness_relative_result)
```


**2. Date must be within 5 days *before* to 6 hours *after* reference:**
```python
timeliness_relative_rule = TimelinessRelativeRule(
    field="date",
    reference_date="2023-01-01",
    start_timedelta='-5d',      # 5 days before
    end_timedelta='+6h',        # up to 6 hours after
)
```


**3. Compare against another column (per-row):**
Suppose your DataFrame has `delivery_date` and `order_date`.  
Check that `delivery_date` is within 10 days after `order_date`:

```python
timeliness_relative_rule = TimelinessRelativeRule(
    field="delivery_date",
    reference_column="order_date",
    start_timedelta='0d',
    end_timedelta='10d',
)
```
*Each row's `delivery_date` is checked for being on/after `order_date` and no more than 10 days later.*


### Notes

- If **only** `start_timedelta` is set, the upper boundary is infinite
- If **only** `end_timedelta` is set, the lower boundary is infinite
- Both can be combined for an allowed range.
- ISO8601 durations (e.g., `'P5D'` for 5 days, `'PT6H'` for 6 hours) are also accepted.


### 2.6 ValidityRegexRule

Checks if strings follow a pattern (e.g., valid email).
See our [Advanced Tutorial](python-advanced.md) for how to manage your Regex patterns for production.

```python
validity_regex_rule = ValidityRegexRule(
    field="email", regex_pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
)
validity_regex_result = validity_regex_rule.evaluate(df)
print(f"Regex validity pass rate: {validity_regex_result.pass_rate}")
FINAL_REPORT.results.append(validity_regex_result)
```



### 2.7 ValidityNumericalRangeRule

Checks if numbers fall within a range.
If left blank / None it replaces with -infinity (min_value) or +infinity (max_value)

```python
validity_numerical_range_rule = ValidityNumericalRangeRule(
    field="age", min_value=1, max_value=120
)
validity_numerical_range_result = validity_numerical_range_rule.evaluate(df)
print(f"Numerical range validity pass rate: {validity_numerical_range_result.pass_rate}")
FINAL_REPORT.results.append(validity_numerical_range_result)
```



## 3. Combine & Export Results

You can turn your results into a DataFrame (or other formats) for reporting:

```python
df_report = FINAL_REPORT.to_dataframe()
df_report.to_csv("data_quality_report.csv", index=False)
```

Format options for display/reporting, e.g. to make Excel row numbers line up with the records_failed_ids we can shift them.
```python
df_report = FINAL_REPORT.to_dataframe(
    decimals=2, 
    measurement_time_format="%Y-%m-%d %H:%M", 
    records_failed_ids_shift=2
)
```



## 4. DataQualityConfig (optional/advanced)

Instead of manually appending, you can define rule sets as a config object:

```python
dq_config = DataQualityConfig(
    dataset_name="Tutorial Data",
    lifecycle_stage="02 Post Processing",
    measurement_time="2025-01-01",
    rules=[
        uniqueness_rule,
        completeness_rule,
        accuracy_rule,
        timeliness_relative_rule,
        timeliness_static_rule,
        consistency_rule,
        validity_numerical_range_rule,
        validity_regex_rule,
    ],
)

dq_report = dq_config.execute(df)
df_report = dq_report.to_dataframe()
df_report.to_csv("data_quality_report.csv", index=False)
```



## 5. Next Steps

- To learn about advanced configuration, using YAML rules files, and automation, continue with [Advanced Python Tutorial](python-advanced.md).

- Generate rule definitions from your report using:
    ```python
    dc = DataQualityConfig.from_report(dq_report)
    dc.to_yaml("rules.yaml", overwrite=True) # save the rules to a YAML file and use this a fast way of creating a template for changing them
    ```