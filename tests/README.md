# Overview

The majority of tests are organised by having a YAML file of test examples which are loaded when pytest is run.

For example, for the function 'uniqueness()', there is an equivalent YAML file called 'uniqueness.yaml' which contains the inputs to the function and expected outputs.

WHen pytest runs, we generate a pytest feature of cases from this YAML file (within conftest.py), and this is called 'uniqueness_case'.

We then pass 'uniqueness_case', which is a list of inputs and expected outputs into test_uniqueness().

This means adding additional edge cases can be achieved my modifying the YAML file, and if you want to understand what a function does, you can do so from reading the input > output of each YAML file.

## Spark testing
We use the same test cases in both spark and pandas for our top level data quality functions.

We have some helper functions in spark.confest.py to
create a spark dataframe from the test cases (we have to infer the schema based on the inputs)

We have some tests unique to Spark, focussing on nested data.