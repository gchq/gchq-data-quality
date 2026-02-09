# Data Quality

Welcome! This package is designed as a simple and fast way for engineering teams with billions of records, as well as those of you working in Jupyter Notebooks, to quickly and repeatably measure the quality of your data.

If we record data quality the same way, it's easier to share results with each other and we can re-use existing resources like dashboards.

## Our motiviation

We wanted a data quality package that fulfilled these aims:

- **Open-Source & Permissive**: Licensed under Apache 2.0, with no commercial strings attached - opensource forever.
- **Simplicity First**:  Teams can get started fast — plug in your DataFrame, define rules, and get insights with minimal code. What you do in a jupyter notebook is the same code as in production at scale. How you schedule, sample, alert and visualise is up to you (and out of scope in this package)
- **Handles Nested Data**: Supports Spark DataFrames with nested data.
- **Comparisons between values**: Able to compare values with a range of logical operators between different columns, especially involving time of events.
- **Built for Insight**: Outputs a standard tabular format, designed to give useful insights and be easy to visualise in a dashboard. You should compare results from different rules on the same field to help diagnose data quality errors, this is more insightful than the number of rules that passed or failed some threshold.

### Acknowledgements
We are grateful for DAMA-UK (Data Management Association, UK Chapter) for granting us permission to reference and use their Data Quality Dimensions throughout the tutorials and code.
Source: Dama International. 2017. DAMA-DMBOK: Data Management Body of Knowledge (2nd Edition). Technics Publications, LLC, Denville, NJ, USA.