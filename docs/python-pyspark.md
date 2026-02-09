# Data Quality Tutorial - PySpark

## Prerequisites

- Have worked through **[Python 1](python-basic.md) and [Python 2](python-advanced.md) Tutorials**:
    - Able to run and interpret data quality functions.
    - Comfortable managing YAML rule configs.
- Basic experience with Spark and pandas DataFrames.

## Aim

- Run data quality rules directly on Spark DataFrames.
- Understand how nested data is handled in Spark within this package.


## 1. Example Data

Create the test dataset (same as Python 1, but for Spark):

```python
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession

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

spark = SparkSession.builder.appName("My App").getOrCreate()
dfs = spark.createDataFrame(df)
dfs.show()
```



## 2. Running Data Quality Rules in Spark

The API is identical to pandas

```python
from gchq_data_quality import UniquenessRule, TimelinessStaticRule

uniqueness_rule = UniquenessRule(field="id")
dq_result = uniqueness_rule.evaluate(dfs)
print(dq_result.model_dump())
```

### Note:
- Under the hood, we split the data into lots of small dataframes and pass to a Spark worker (with the exception of UniquenessRule which we measure using native Spark). This was done to minimise our codebase - we can reuse the majority of our pandas code.
- `records_failed_ids` is not returned for Spark DataFrames (Spark DataFrames are inherently unordered).
- You will not get python objects in `records_failed_samples` but rather a JSON serialisation of them, e.g. datetime strings rather than datetime objects


### Running a Whole YAML Config in Spark

You can re-use your YAML config and regex pattern files:

```python
from gchq_data_quality import DataQualityConfig

dq_config = DataQualityConfig.from_yaml(
    file_paths="SOLUTION_rules_with_regex.yaml",
    regex_yaml_path="regex_patterns.yaml"
)
dq_report = dq_config.execute(dfs)
dq_report.to_dataframe()
```

**You can also repartition the Spark DataFrame to control the parallelism:**

```python
dfs_2 = dfs.repartition(2) # override Spark's default
dq_report = dq_config.execute(dfs_2)
dq_report.to_dataframe()
```


## 3. Under the Hood

- **Pandas & Spark parity:** Most rules are run by partitioning Spark data to small pandas DataFrames, processed in parallel using `mapInPandas`. Therefore take care with any expression in your ConsistencyRule that might use values like `ColumnA.mean()` - as this mean value will be based on a parition, not the whole dataset!
- **Uniqueness:** Runs in pure Spark (not split by partition, as we need knowledge of all values).
- **Measurement time:** Each partition is measured independently; `measurement_time` from the latest partition is used when we aggregate the results back.


## 4. Handling Nested Data

A unique strength of this package is **support for deeply nested data** (arrays, structs).

### Example: Pet Shop Customers

```python
from pyspark.sql.types import *

data = [
    {
        "id": 1,
        "customers": {
            "name": "John",
            "age": 30,
            "pets": [
                {
                    "name": "Fido",
                    "appointments": [
                        {"date": "2022-01-01", "comment": "Fido First appointment"},
                        {"date": "2022-01-02", "comment": "Fido Second appointment"},
                    ],
                },
                {
                    "name": "Whiskers",
                    "appointments": [
                        {"date": "2022-02-03", "comment": "Whiskers First appointment"},
                        {
                            "date": "2022-02-04",
                            "comment": "Whiskers Second appointment",
                        },
                    ],
                },
            ],
        },
    },
    {
        "id": 2,
        "customers": {
            "name": "Jane",
            "age": 25,
            "pets": [{"name": "Rex", "appointments": []}],
        },
    },
    {
        "id": 3,
        "customers": {
            "name": "Mr No Pets",
            "age": 102,
            "pets": [{"name": None, "appointments": []}],
        },
    },
    {
        "id": 4,
        "customers": {
            "name": "Mrs Missing Pets",
            "age": 15,
            "pets": [
                {"name": "missing", "appointments": [{"date": None, "comment": "none"}]}
            ],
        },
    },
]

schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField(
            "customers",
            StructType(
                [
                    StructField("name", StringType(), True),
                    StructField("age", IntegerType(), True),  # <-- added age to schema
                    StructField(
                        "pets",
                        ArrayType(
                            StructType(
                                [
                                    StructField("name", StringType(), True),
                                    StructField(
                                        "appointments",
                                        ArrayType(
                                            StructType(
                                                [
                                                    StructField(
                                                        "date", StringType(), True
                                                    ),
                                                    StructField(
                                                        "comment", StringType(), True
                                                    ),
                                                ]
                                            )
                                        ),
                                        True,
                                    ),
                                ]
                            )
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
    ]
)
df_pets = spark.createDataFrame(data, schema=schema)
df_pets.printSchema()
df_pets.show()
```

### Flattening and Referencing Nested Data

- Data is automatically **flattened** for DQ checks. You can inspect this flattening process beforehand using `flatten_spark`.

- Use "dotted" field notation, with:
    - `[*]` for "all values"
    - `[]` for "first non-null value"

**Examples:**

```yaml
field: customers.pets[*].name           # every pet's name for customers
field: customers.pets[].name            # just the first pet's name (if any)
```

Column name translation when flattened:

- `.` becomes `_`
- `[*]` becomes `_all`
- `[]` becomes `_first`

So `customers.pets[*].name` becomes `customers_pets_all_name` in the flattened DataFrame.

**You can flatten explicitly to check:**

```python
from gchq_data_quality.spark.dataframe_operations import flatten_spark

df_flat = flatten_spark(df_pets, flatten_cols=["id", "customers.name", "customers.pets[*].name"])
df_flat.show()
```

### Writing DQ Rules for Nested Data

In your YAML configuration, use nested notation for `field`:

```yaml
- field: customers.pets[].name
  function: completeness
- field: customers.age
  function: validity_numerical_range
  min_value: 18
  max_value: 120
- field: customers.pets[*].appointments[*].date
  function: timeliness_static
  start_date: 2022-01-01
  end_date: 2023-01-01
- field: customers.name
  function: consistency
  expression:
    if: '`customers.age` < 18'
    then: '~`customers.name`.str.startswith("Mr")'
```

**Remember your backticks around all column names**

Run the config as before:

```python
nested_config = DataQualityConfig.from_yaml(
    "nested_data_rules.yaml",
    regex_yaml_path="regex_patterns.yaml"
)
dq_pets_nested_report = nested_config.execute(df_pets)
dq_pets_nested_report.to_dataframe()
```

## 5. PySpark Specifics & Tips
- It's OK to sample, you don't need to measure all your data to get a repeatable data quality pass rate; experiment to find the best sample size.
- **No records failed IDS (row numbers):** Invalid row numbers are not reported (Spark DataFrames are unordered).
- **Column name mapping:** After flattening, columns are renamed (`.` -> `_`, etc). If generating configs from a report, you may have to "reverse" the renaming when moving from report output to config file.
- **DataFrame-wide statistics:** Consistency rules using group statistics can be unreliable on partitioned data (e.g. `col1 <= other_col.mean()`). For reliable results, repartition to a single worker or precompute the statistic.
- **Timezones:** Always coerce to UTC. Spark can assign local timezones in datetimes without a timezone (depending on local configuration) and cause subtle errors in DQ Timeliness rules.

## 6. Production Ready!

Thank you for getting this far

