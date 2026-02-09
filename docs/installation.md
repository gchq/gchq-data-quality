# Installation

The `gchq-data-quality` package is designed to be modular, allowing you to install its core feature (Pandas dataframe code) as well as optional components for Spark (`pyspark`) and/or Elasticsearch (Elasticsearch is not yet implemented).

## Prerequisites

Before installing `gchq-data-quality`, ensure the following:
- **Python**: Version 3.11 or later is required.
- **Package Manager**: `pip` (Python's package manager) should be up-to-date. Run:
  ```shell
  pip install --upgrade pip
  ```

## Installation Steps

### Core Package

To install the core functionality of `gchq-data-quality` using Python's `pip`, run:
```shell
pip install gchq-data-quality
```

### Optional Dependencies

#### PySpark Integration

To use `gchq-data-quality` with **Apache Spark**, install the package with the `pyspark` extra:
```shell
pip install gchq-data-quality[pyspark]
```

This additionally installs:

- **pyspark** (this is quite a large package, hence having it as an optional extra)

#### Elasticsearch Integration

To use `gchq-data-quality` with **Elasticsearch**, install the package with the `elasticsearch` extra:
```shell
pip install gchq-data-quality[elasticsearch]
```

This additionally installs:

- **elasticsearch**

#### Multiple Extras

You can combine multiple optional integrations in a single command. For example, to install both PySpark and Elasticsearch support:
```shell
pip install gchq-data-quality[pyspark,elasticsearch]
```

## Development Installation

For local development, clone the repository and install the package in **editable mode**:

1. Clone the repository:
   ```shell
   git clone https://github.com/gchq/gchq-data-quality.git
   cd gchq-data-quality
   ```

2. Install dependencies:
   ```shell
   pip install -e ".[dev]"
   ```

The `dev` group includes:
- Testing tools (`pytest`, `coverage`)
- Pre-commit hooks (`pre-commit`)
- Formatting and linting tools (`ruff`)
- `ipykernel` so you can edit the Tutorial Notebooks if required
- Packages to build documentation with MkDocs.

## Verification and Testing

After installation, verify that the package was installed correctly:

```shell
python -c "import gchq_data_quality; print(gchq_data_quality.__version__)"
```

## Uninstall

To uninstall the package and its dependencies, run:
```shell
pip uninstall gchq-data-quality
```

If you installed optional dependencies, repeat the command for each extra name (`pyspark`, `elasticsearch`, etc.).