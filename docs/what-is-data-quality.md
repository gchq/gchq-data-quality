# What is Data Quality?

**Data quality** refers to how well data meets the expectations and needs of its consumers. Data is considered **high quality** when it is **fit-for-purpose**—in other words, when it supports its intended use effectively and reliably. It is unlikely that the quality of data will ever be perfect.

In practice, we define data quality as the percentage of records that pass a set of data quality rules you define, based on how you will use the data.



## The DAMA Six Dimensions of Data Quality

We follow the **DAMA Framework**, grouping rules under six core dimensions. We will consider a company measuring its HR records in our examples:

| Dimension    | Practical Definition                                                                                      |
|--|-|
| **Uniqueness**   | Checks there are no duplicates (e.g., employee number should be unique).                                 |
| **Completeness** | Ensures each required value is present (i.e., not NULL, blank, or 'N/A').                                |
| **Accuracy**     | Data correctly reflects the real-world "truth" (usually by checking against an authoritative source or by spot-checking). Example: using ISO country codes, verifying birth dates from certificates. In our code, we just check against a known list. Whilst this doesn't guarantee it's accurate, if it fails we know it must be in-accurate. It is useful for insights to split apart regular expression checks form authoritative list checks, so we use accuracy for this purpose. |
| **Validity**     | Data matches expected syntax (format, type, or range)—e.g., positive ages, valid email addresses — regular expressions or range checks. |
| **Timeliness**   | Data is up-to-date and times are plausible (e.g., all records “fresh” enough to be useful). |
| **Consistency**  | Logical relationships between data points hold true (e.g., date of birth is before date of death; codes match reference values in other datasets). |

## What is the right dimension to use?
We have found that people want to pick the correct data quality dimension for a particular rule. As the DAMA definitions are not specified as code in the DAMA Book it leaves some things open to interpretation.

Take date of birth. If it is in the future, is that a failure on timeliness, validity, accuracy or consistency? One could argue that it is both invalid, inaccurate and inconsistent.

We take the approach (supported by our code - the dimension can be overwritten for any rule), that you should just pick a dimension and apply it consistently, especially if you are measuring the same data at different points along its lifecycle. It's less important that the dimension is 'correct', but more important to gain useful insights and have a common understanding in whoever looks at the data.

Basically don't worry about it too much!

## How We Measure Data Quality

For each rule:

- The pass rate is a **number from 0 to 1** (the proportion of checked records that pass).
- Pass rates can be **aggregated** to track trends over time, or compare across fields and dimensions.
- Monitoring these pass rates, between dimensions and rules, helps diagnose and improve data pipelines. For example, if the uniqueness score drops with no other changes, it's likely you are getting duplicate records. If completeness and validity both plummet, it's likely your ingest pipeline needs fixing.

## Data Quality Report Columns

A Data Quality Report contains:

| Field                 | Description                                                                    |
|--|--|
| `dataset_name`        | Human-readable name for your dataset.                                          |
| `dataset_id`          | Machine-readable identifier for the dataset.                                   |
| `measurement_sample`  | Description of the sample (e.g. “10% records”).  |
| `lifecycle_stage`     | Stage in the data lifecycle being measured (e.g., “01_ingest”, “02_enrich”).   |
| `measurement_time`    | Timestamp (UTC) of when the measurement was taken.                             |
| `field`               | The specific field/column being checked.                                       |
| `data_quality_dimension` | Which DAMA dimension this rule measures (e.g., Completeness, Validity).      |
| `rule_id`             | Short machine-readable identifier for the rule.                                |
| `rule_description`    | Human-readable explanation (e.g., “value matches email pattern”).              |
| `rule_data`           | Full machine-readable rule definition (enough to recreate the rule). We store as JSON. |
| `pass_rate`           | Fraction of records that passed this rule (between 0 and 1).                   |
| `records_evaluated`   | Number of records checked for this rule.                                       |
| `records_failed_ids`  | List of indices (row numbers) of failed records (for troubleshooting).         |
| `records_failed_sample` | Example values from records that failed (diagnostic sample).                   |



## Why Data Quality Matters

This will be obvious to anyone who tries to analyse poor quality data! A few examples are:

- **Increased efficiency** – High-quality data reduces time spent cleaning and fixing downstream.
- **Fewer errors and inconsistencies** – Helps catch upstream schema changes, and other problems early.
- **Improved compliance** – Satisfies internal, legal, or external data quality requirements. e.g. retention limits of records.



Whilst measuring the quality of data takes some work, overall it will save you time. Have an outage and want to know if the data has changed when it's back online? If you run the same rules and get the same answer, you can have an evidence-based answer in seconds.