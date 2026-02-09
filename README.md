# GCHQ Data Quality Package

## Why build our own Data Quality Package?
There are a number of existing commercial and opensource data quality (DQ) packages already available. We created our own for five reasons:
### 1. Opensource under Apache 2.0

For permissive use.

### 2. Simplicity.

We opted for a simple 'plug-in' approach to DQ measurement to speed up adoption by Engineering teams:

1. Get your data into a dataframe (Pandas or Spark)
2. Define some rules (using only 8 functions)
3. It will output a dataframe of results

How you handle the workloads either side are up to you (connecting to data, scheduling, sampling, dashboarding, alerting etc.)

We deliberately ignore connections to SQL / MongoDB etc, as all of those can, via an SDK, get data into a dataframe.

### 3. Handle Nested Data

A lot of our data is nested. No other open-source data quality package handles nested data (that we could find).

### 4. Better Data Quality rules for comparing two values.

We wanted something that:

1. Gave us granular comparisons around time (such as an event happening within a time window relative to another date - important for us)
2. Used pandas.eval() syntax to provide a huge range of flexibile rules with logical operators, summary statistics, string and datetime operations. We maximise the use of this powerful syntax, without complicating our code

### 5. Designed for Insights

Other packages are designed with an Engineering mindset - it's about the number of rules that pass or fail. This isn't great for diagnosing the root cause of data quality issues. Our results format is a flat table, with enough metadata to make it easy to:

1. Build an insightful dashboard
2. Work out the cause of the DQ problem

## Tutorials

There are tutorial notebooks to guide you through using the code. See the tutorials/ directory to find notebooks to download.

## Orientation

### Language
Consistent verbs / descriptions for both the code and the output are used.

A dataset is the data you are measuring, it may have a name and an ID.
Data Quality is formed from a set of Rules - these are **evaluated** can be passed or failed by a record in your dataset.

Rules are combined into a data quality configuration, which is **executed** on a dataset, and explains when the measurement happened, at one stage of the lifecycle etc.

The measure we use is the pass rate (records passing / records evaluated). This makes it distinct from a score
which suggests weighting. Users are free to take the pass rate and create weighted scores if they wish, although we do not recommend doing this as it is more confusing to interpret.

# Acknowledgements
We are grateful for DAMA-UK (Data Management Association, UK Chapter) for granting us permission to reference and use their Data Quality Dimensions throughout the tutorials and code.
Source: DAMA International® (2017) DAMA-DMBOK®: Data Management Body of Knowledge.
ISBN 9781634622349. Copyright registered June 18, 2018 (TX0008608498).
Rights and permissions: DAMA International®, 2512 E. Evergreen Blvd, #1023,
Vancouver, WA 98661, United States. (973) 625-4347. 


# Data Quality Tools — Python Quickstart

## Basic Usage

### 1. Import Rules

Import the rule classes and supporting components:

```python
import pandas as pd
from gchq_data_quality import (
    UniquenessRule,
    CompletenessRule,
    ValidityRegexRule,
    DataQualityConfig,
)
```

### 2. Create Example Data

```python
df = pd.DataFrame({
    "id": [1, 2, 2, 3],
    "email": ["a@example.com", "b@sample.com", "invalid@", None],
})
```

### 3. Define and Evaluate Quality Rules

Instantiate each rule for the field of interest, and evaluate against your DataFrame:

```python
# Uniqueness
uniqueness_rule = UniquenessRule(field="id")
uniqueness_result = uniqueness_rule.evaluate(df)
print("Uniqueness:", uniqueness_result.pass_rate)  # e.g. 0.75 if 3/4 unique

# Completeness
completeness_rule = CompletenessRule(field="email")
completeness_result = completeness_rule.evaluate(df)
print("Completeness:", completeness_result.pass_rate)

# Validity using regular expressions
validity_rule = ValidityRegexRule(
    field="email", regex_pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
)
validity_result = validity_rule.evaluate(df)
print("Email validity:", validity_result.pass_rate)
```

### 4. Running Multiple Rules via a Config Object

You can bundle your rules and dataset details in a `DataQualityConfig`, execute them all at once, and get a tabular report:

```python
dq_config = DataQualityConfig(
    dataset_name="My Example Dataset",
    rules=[
        uniqueness_rule,
        completeness_rule,
        validity_rule,
    ],
)
dq_report = dq_config.execute(df)
print(dq_report.to_dataframe())
```

## More Rule Types

Other measurements are available (AccuracyRule, ValidityNumericalRangeRule, ConsistencyRule, TimelinessStaticRule, etc.) — see notebook examples and code documentation for details.

## YAML Configurations

For defining rules in YAML and running configs across multiple datasets, see the advanced Python 2 tutorial.

### Spark dataframes

The same approach is used in Spark dataframes, and with Spark you can also handle nested data.
```python
from pyspark.sql import SparkSession

from gchq_data_quality import DataQualityConfig
from gchq_data_quality.spark.dataframe_operations import flatten_spark

spark = SparkSession.builder.getOrCreate()

# Create nested example data: 2 parents, one with 1 child, one with 2
data = [
    {
        "parent": {
            "age": 40,
            "children": [{"age": 10}]
        }
    },
    {
        "parent": {
            "age": 35,
            "children": [{"age": 7}, {"age": 5}]
        }
    }
]

schema = StructType([
    StructField("parent", StructType([
        StructField("age", IntegerType(), True),
        StructField("children", ArrayType(StructType([
            StructField("age", IntegerType(), True)
        ])), True),
    ]), True)
])

spark = SparkSession.builder.getOrCreate()
spark_df = spark.createDataFrame(data, schema=schema)

# Optionally flatten nested columns to inspect structure - worth checking it's what you expected. Deeply nested data can get quite confusing.
df_flat = flatten_spark(
    spark_df, flatten_cols=["parent.age", "parent.children[*].age"]
)
df_flat.show()

# Load DQ rules from YAML config (rules.yaml, e.g., we might run a consistency rule to check that '`parent.age` > `parent.children[*].age`
# i.e. all parents are older than all of their children
# See the tutorial for examples

dq_config = DataQualityConfig.from_yaml("rules.yaml")

# Run all configured rules
dq_report = dq_config.execute(spark_df)
print(dq_report.to_dataframe())

spark.stop()
```

# Data Quality Overview

A brief overview of what we mean when we say 'data quality'
## What is Data Quality?

Data quality refers to how well data meets the expectations and needs of its consumers.  
Data is considered **high quality** when it is **fit-for-purpose** – meaning it supports the intended use effectively and reliably.

High-quality data provides:
- Assurance of compliance (policy or law).
- Confidence that analysts are receiving accurate and usable information.
- Early detection of data processing issues, such as:
  - Faulty data pipelines
  - Upstream schema changes
  - Poor data entry
  - Time-based anomalies (e.g., reduced data quality at weekends)

That's all quite theoretical. But in a practical sense, we define it as the percentage of records that pass a specific data quality rule. It's a 'unit test' for data.

## Key Dimensions of Data Quality

We measure data quality by the percentage of defined rules that a record passes.  
Rules are grouped under the **DAMA Framework’s six dimensions**:

1. **Uniqueness** – No duplicates (e.g., every ID should be unique).
2. **Completeness** – Data fields are present and not empty (e.g., no NULL or ‘N/A’ values).
3. **Accuracy** – Values correctly describe the real-world object or event, usually checked against an authoritative dataset. e.g 'country' is an actual ISO country code.
4. **Validity** – Values conform to the required format, type, or range (e.g., valid email address, age within a reasonable range).
5. **Timeliness** – Data is up-to-date and time values make sense (e.g., birth dates are in the past).
6. **Consistency** – Logical consistency within or across datasets (e.g., date of birth should be before date of death).

## How We Use Data Quality Measures

- Each rule produces a score between `0` and `1` based on the proportion of records that pass.
- Scores can be monitored over time to detect trends and changes.
- Scores can be aggregated over the DAMA Dimensions, over fields or source data (makes it easy to dashbaord)
- Drops or spikes in scores may signal problems in:
  - Extraction and transformation pipelines
  - Upstream schema changes
  - The relative changes to score across the dimensions can indicate what type of issue it is. e.g. a drop in uniqueness but no other change, suggest being given duplicate data. A drop in completeness, but not in validity, suggest a problem at data entry, with fields not being populated.

## Why Data Quality Matters

Good data quality supports:
- More confident decision-making (higher assurance of underlying data)
- Reduced confusion from duplicate or invalid records
- More efficient data engineering work (if data is high quality, then joining, transformations, enrichment become easier)


