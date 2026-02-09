# Contributing

Thank you for considering contributing to `gchq-data-quality`.

## Ways to Contribute

- **Raise Issues**: Report bugs you encounter or suggest feature improvements via [GitHub Issues][github-issues].
- **Submit Pull Requests (PRs)**:
  - Develop solutions to open issues or suggest features.
  - Improve documentation and provide examples of usage.
- **Spread Awareness**: Share this library with others in the data quality community.
- **Scope of package**: Please try to stick to the philosophy of the package regarding 1) simplicity and 2) rule structure. Rules should *all* be of the type where a record can either pass or fail. Out of scope is rules that assess an entire dataset, such as the average value of a column, or the distribution of categorical variables. These would fall into topic of the profile / distribution of the data. It would of course be possible to create a dataframe of metrics and then apply the existing rules to on those metrics, if this was functionality you wanted.

All contributors must adhere to our [Code of Conduct](CODE_OF_CONDUCT.md).

## Setting Up Your Environment

We use `uv` for dependency management. Here's how to set up your environment:

1. Install `uv`:
   ```shell
   pip install uv
   ```
2. Sync dependencies:
   ```shell
   uv sync --all-extras
   ```
   `gchq-data-quality`runs on Pandas and Spark dataframes. We don't want to force users to install pyspark if it's not needed, so pyspark is an 'extra' within our `pyproject.toml` file. Elasticsearch is also an extra, but this feature is not yet implemented.

   `--all-extras` will install both pyspark and elasticsearch packages, which is recommended for dev work.

3. You can:
   - Run commands in a virtual environment with `uv run <command>`:
     ```shell
     uv run pytest tests/
     ```
   - For IDEs, set the Python executable in `.venv/bin` (Linux) or `.venv/Scripts` (Windows) as your active interpreter.

To add packages as dependencies, update `pyproject.toml` and run:
```shell
uv sync
```

## Pre-Commit Hooks

To ensure code consistency and catch issues early, we use [pre-commit hooks][pre-commit]. Install and activate them with:
```shell
pip install pre-commit
pre-commit install
```

Our hooks are automatically run during pull requests for:
- Checking copyright (via `scripts/check-copyright.sh`).
- Clearing output cells in Jupyter notebooks.
- Linting and formatting with [ruff][ruff].

These hooks can also be run manually with:
```shell
pre-commit run --all-files
```

## Reporting Issues

Before reporting:
1. **Search Open and Closed Issues**: Avoid duplicates by checking [existing issues][github-issues].
2. **Ensure Updates**: Make sure you're using the latest version of `gchq-data-quality`.

## Pull Requests (PRs)

We follow a **GitHub Flow** development approach for `gchq-data-quality`. Here's how to contribute via PRs:

- Changes should relate to an existing issue. Create an issue if it doesn't exist yet.
- Work in a feature branch:
  - Use `feature/<feature-name>` for new features or refactoring.
  - Use `fix/<bug-name>` for bug fixes.
- Ensure changes are limited to related files and meet the following criteria:
  - Sufficient test coverage (use `pytest` and `coverage` to verify).
  - Conformity to style guides (`ruff` and `pre-commit hooks`).
  - Add related entries to `CHANGELOG.md`.

To avoid merge conflicts, pull from `main` often:
```shell
git fetch origin
git pull origin main
```

### Pull Request Workflow:
1. Open a [Draft Pull Request][pr-draft] to keep maintainers informed.
2. Refine your changes, ensuring all pre-commit hooks pass.
3. Change your PR to **Ready for Review** when complete.

---

## Testing

Tests for `gchq-data-quality` are located in the `tests/` directory and can be run using `pytest`. Here's the structure:

- `tests/`: Main directory for unit tests.
- `tests/spark/`: Spark-specific tests (requires `pyspark`).
- `tests/elasticsearch/`: Elasticsearch-specific tests (requires `elasticsearch`).

### Running Tests:
```shell
uv run pytest tests/  # Run all tests
```

For specific extras:
- **Spark Tests**:

```shell
  uv sync --extra pyspark
  uv run pytest tests/spark/
  ```

- **Elasticsearch Tests**:
(Not yet implemented)

```shell
  uv sync --extra elasticsearch
  uv run pytest tests/elasticsearch/
  ```

We use pytest for testing, and `pre-commit` ensures proper linting and formatting.

## Style Guide

Code should follow these guidelines:
- Formatting and linting are handled by ruff (configured in `pyproject.toml`).
- Follow PEP8 for general style, except where specified in `ruff` ignores.
- Use type annotations for all functions.

### Docstring Standards

Docstrings should follow the [**Google Python Docstring Style**](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings).

## Documentation

Complete and clear documentation is important for easy usage of `gchq-data-quality`. One of the main aims of the project is to get engineering teams up and running with data quality in as little time as possible. Please ensure any new features are documented with examples. If a significant new feature, add it to a tutorial notebook (or create a new tutorial).

### Writing Documentation

We use [MkDocs](https://www.mkdocs.org/) for generating documentation, and documentation content is written in **Markdown files**.

The documentation source files are located under the `docs/` directory of the project. When creating or updating documentation:
- Use Markdown syntax.
- Ensure all relevant sections are linked properly.
- Include examples and usage instructions where applicable.

New files should follow the structure of the existing documentation and be placed within appropriate subdirectories under `docs/`.

### Building the Documentation

To build the documentation locally:
1. Ensure documentation dependencies are installed:

   ```shell
   uv sync --group docs
   ```

2. Run MkDocs to build the documentation:
   ```shell
   mkdocs build
   ```

The static site will be generated under the `site/` directory. You can preview the site locally using:
```shell
mkdocs serve
```
The above command starts a local development server, typically at `http://127.0.0.1:8000`, where you can preview changes live.


## GitHub Actions

Code submissions are verified via GitHub Actions, which run tests across **Linux**, **macOS**, and **Windows** platforms. The testing matrix covers the following:

- **Linux**: Full tests for Pandas, Spark & Elasticsearch
- **macOS**: Full tests for Pandas, Spark & Elasticsearch
- **Windows**: Limited to Pandas tests (unusual to have Spark or Elasticsearch setup on a Windows machine).


[github-issues]: https://github.com/gchq/gchq-data-quality/issues
[pr-draft]: https://docs.github.com/en/pull-requests