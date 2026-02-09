# (c) Crown Copyright GCHQ \n
r"""
Data quality configuration model - this stores details about the measurement event (when, on what)
and contains a list of rules to evaluate against the data source.

User-Facing Model:
    - DataQualityConfig: Primary configuration class for specifying datasets and associated data quality rules, used for running quality checks and serialising to/from YAML/JSON.

Rule classes (imported from gchq_data_quality.rules) define specific data quality checks and are typically instantiated in advance of the DataQualityConfig.

This module provides:
    - DataQualityConfig for describing data quality measurement settings and rules.
    - Support for execution against pandas DataFrames and Spark DataFrames (Elasticsearch indices not yet implemented).

Usage Example:
    ```python
    from gchq_data_quality.rules import ValidityRegexRule
    from gchq_data_quality.config import DataQualityConfig

    rule = ValidityRegexRule(field="email", regex_pattern=r"[^@]+@[^@]+\.[^@]+")
    config = DataQualityConfig(dataset_name="customer_data", rules=[rule])
    report = config.execute(dataframe)

    The config can be recreated from the report (via a JSON load of each rule found in the rule_data attribute):
    config = DataQualityConfig.from_report(report)
    ```
"""

from __future__ import annotations

import json
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, overload

import pandas as pd
import yaml

if TYPE_CHECKING:
    from elasticsearch import Elasticsearch
    from pyspark.sql import DataFrame as SparkDataFrame

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
)

from gchq_data_quality.models import (
    UTCDateTime,
)
from gchq_data_quality.results.models import DataQualityReport, DataQualityResult
from gchq_data_quality.rules.accuracy import AccuracyRule
from gchq_data_quality.rules.completeness import CompletenessRule
from gchq_data_quality.rules.consistency import ConsistencyRule
from gchq_data_quality.rules.timeliness import (
    TimelinessRelativeRule,
    TimelinessStaticRule,
)
from gchq_data_quality.rules.uniqueness import UniquenessRule
from gchq_data_quality.rules.validity import (
    ValidityNumericalRangeRule,
    ValidityRegexRule,
)

# Union type for all possible rules
RuleType = Annotated[
    UniquenessRule
    | CompletenessRule
    | ValidityRegexRule
    | ValidityNumericalRangeRule
    | ConsistencyRule
    | AccuracyRule
    | TimelinessRelativeRule
    | TimelinessStaticRule,
    Field(discriminator="function"),
]

# allows validation of a dictionary into the correct rule type based on the 'function' key
RuleAdapter = TypeAdapter(RuleType)


class DataQualityConfig(BaseModel):
    """
    Configuration describing a set of data quality checks to be run on a dataset.

    Typically constructed by loading a YAML file specifying the dataset and a list
    of rule definitions. Can also be created programmatically.

    Attributes:
        dataset_name (str | None): Dataset name or identifier.
        measurement_sample (str | None): Description of data sample.
        lifecycle_stage (str | None): The lifecycle stage at which data is measured.
        measurement_time (datetime | None): Measurement timestamp.
        dataset_id (str | int | float | None): Local data catalogue ID.
        rules (list[RuleType] | None): List of rule models.

    Example:
        ```python
        # Loading from YAML
        config = DataQualityConfig.from_yaml("my_config.yaml")
        -- see tutorial for how to specify the yaml file, or create a config programatically and use .to_yaml() to create something to start with.

        # Override regex patterns
        config = DataQualityConfig.from_yaml("my_config.yaml", regex_yaml_path='regex_patterns.yaml')

        # Running data quality checks
        report = config.execute(data_source=my_dataframe)

        # Or, creating config programmatically from scratch
        config2 = DataQualityConfig(
            dataset_name="my_data",
            rules=[
                ValidityRegexRule(field="email", regex_pattern='.+@example.com'),
            ],
        )
        ```

    Methods:

        execute(data_source) -> DataQualityReport:
            Execute the measurement configuration against the provided data source
            (e.g., pandas DataFrame, Spark DataFrame).
            Runs each rule's evaluate() method and returns a DataQualityReport
            containing the results.

        from_yaml(file_path: str | Path, regex_yaml_path: str | Path | None = None) -> DataQualityConfig:
            Load a configuration instance from a YAML file. If regex_yaml_path is provided,
            regex patterns in rule definitions can be overridden or supplemented by patterns
            from this separate YAML file.

        to_yaml(file_path: str | Path, overwrite: bool = False) -> None:
            Save as YAML file.

        from_report(report: DataQualityReport) -> DataQualityConfig:
            Create config instance from report results. This will extract the rule defintition from the
            rule_data field (which is a JSON dump of all rule metadata)
    """

    model_config = ConfigDict(
        extra="forbid"
    )  # Prevents typos in the YAML file from passing validation (.e.g source data instead of dataset_name)

    dataset_name: str | None = Field(
        default=None, description="Name or identifier of data source"
    )
    dataset_id: str | int | float | None = Field(
        default=None, description="Local data catalogue ID"
    )
    measurement_sample: str | None = Field(
        default=None, description="Description of data sample measured"
    )
    lifecycle_stage: str | None = Field(
        default=None, description="The stage of the lifecycle being measured"
    )
    measurement_time: UTCDateTime | None = Field(
        default=None, description="Time of measurement"
    )

    rules: list[RuleType] = Field(
        default_factory=list, description="List of data quality rules"
    )

    @overload
    def execute(self, data_source: pd.DataFrame) -> DataQualityReport: ...
    @overload
    def execute(self, data_source: SparkDataFrame) -> DataQualityReport: ...
    @overload
    def execute(
        self,
        data_source: Elasticsearch,
        index_name: str = ...,
        query: dict | None = ...,
    ) -> DataQualityReport: ...

    def execute(
        self,
        data_source,
        index_name: str = "",
        query: dict | None = None,
    ):
        """
        Execute all data quality rules defined in the configuration against the specified data source.
        Note: Elasticsearch execution not yet implemented.
        Args:
            data_source (pandas.DataFrame | pyspark.sql.DataFrame | Elasticsearch):
                The data to be checked. Should be a pandas DataFrame, a Spark DataFrame, or an Elasticsearch client.
            index_name (str, optional): Name of the index if using Elasticsearch.
            query (dict, optional): Query DSL dict to select records from the index (Elasticsearch only).
            Defaults to {'query': {'match_all': {}}} if not supplied.

        Returns:
            DataQualityReport: A report containing the result of each data quality rule.

        Example:
            ```python
            # For a pandas DataFrame
            dq_report = config.execute(df)

            # For Spark DataFrames
            dq_report = config.execute(spark_df)

            # For Elasticsearch * not yet implemented *
            dq_report = config.execute(elasticsearch_client, index_name="index")
            ```
        """
        results: list[DataQualityResult] = []

        for rule in self.rules:
            dq_result = rule.evaluate(
                data_source=data_source, index_name=index_name, query=query
            )
            dq_result = _copy_config_values_to_dq_result(self, dq_result)
            results.append(dq_result)

        return DataQualityReport(results=results)

    @classmethod
    def from_report(cls, report: DataQualityReport) -> DataQualityConfig:
        """
        Construct a DataQualityConfig instance from a DataQualityReport, rebuilding rule configurations
        from embedded rule metadata within each DataQualityResult (found as a JSON string in 'rule_data').

        Args:
            report (DataQualityReport): A report object containing results and rule metadata.

        Returns:
            DataQualityConfig: A configuration instance matching the report's metadata and reconstructed rules.

        Raises:
            ValueError: If the report contains no records.
            UserWarning: If metadata fields are inconsistent or a rule cannot be reconstructed from metadata.

        Example:
            ```python
            config = DataQualityConfig.from_report(report)
            ```
        """
        if not report.results:
            raise ValueError("DataQualityReport contains no records.")

        # Rebuild rules list
        ALL_RULES = []
        for entry in report.results:
            if not entry.rule_data:
                warnings.warn(
                    f"No rule_data found for record: {entry}, skipping.", stacklevel=2
                )
                continue
            else:
                try:
                    rule_dict = json.loads(entry.rule_data)
                    rule_obj = RuleAdapter.validate_python(rule_dict)
                    ALL_RULES.append(rule_obj)
                except (ValidationError, json.decoder.JSONDecodeError):
                    warnings.warn(
                        f"Unable to parse rule information from rule_data: {entry.rule_data}, skipping this entry",
                        stacklevel=2,
                    )
                    continue

        config_kwargs = _get_config_values_from_first_record(report)

        return cls(rules=ALL_RULES, **config_kwargs)

    @classmethod
    def from_yaml(
        cls,
        file_paths: str | Path | list[str] | list[Path],
        regex_yaml_path: str | Path | None = None,
    ) -> DataQualityConfig:
        r"""
        Loads data quality configuration files, optionally replacing regular expression patterns from a provided YAML file.

        Args:
            file_paths: Single path or list of paths to configuration file(s) to load.
            regex_yaml_path: Optional path to a YAML file containing regular expression patterns to replace in configuration(s).
                The YAML file must contain key-value pairs such as 'EMAIL_REGEX': '<regex_here>'.
                Use single quotes around the regex value within the YAML file (rather than double quotes or no quotes),
                which lets you use typical regex escape characters like \d for a digit.
                Each matching key will have its value substituted within the data quality configuration(s). Defaults to None.

        Returns:
            The loaded data quality configuration. If multiple files are provided, rules from all configurations are combined,
            while other parameters (such as 'dataset_name' and 'measurement_time') are taken only from the first file.

        Raises:
            FileNotFoundError: If any specified configuration file does not exist.

        Warnings:
            If multiple configuration files are provided, only the 'rules' from subsequent files are merged.
            Items such as 'dataset_name' and 'measurement_time' (and any other non-rule parameters) are taken
            exclusively from the first file.

        Example:
            To substitute regular expression patterns from a separate YAML file into your configuration, do the following:

            The YAML file (regex_patterns.yaml) should look like:
            ```yaml
            PET_NAME_REGEX: '^[A-Za-z]{2,20}$'
            FOOD_WEIGHT_REGEX: '^\d+(\.\d{1,2})?\s?(kg|g|lb|oz)$'
            ```

            ```python
            from gchq_data_quality.config import DataQualityConfig

            config = DataQualityConfig.from_yaml(
                'dq_config.yaml',
                regex_yaml_path='regex_patterns.yaml'
            )
            ```

        """
        return _load_data_quality_config_files(file_paths, regex_yaml_path)

    def to_yaml(self, file_path: str | Path, overwrite: bool = False) -> None:
        """
        Save the DataQualityConfig instance as a YAML file for reuse or sharing.

        Args:
            file_path (str | Path): The destination YAML file path.
            overwrite (bool): Whether to overwrite the file if it already exists.
            If False and the file exists, a FileExistsError is raised.

        Returns:
            None
        """

        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        if file_path.exists() and not overwrite:
            raise FileExistsError(f"File already exists: {file_path}")

        export_dict = self.model_dump()

        # Replace rules with already serialisable versions
        export_dict["rules"] = [rule.to_dict() for rule in self.rules]

        with open(file_path, "w") as f:
            yaml.safe_dump(export_dict, f, sort_keys=False)


def _copy_config_values_to_dq_result(
    config: DataQualityConfig, dq_result: DataQualityResult
) -> DataQualityResult:
    """
    Copy top-level metadata fields from the DataQualityConfig instance to a DataQualityResult instance.

    Parameters:
        config (DataQualityConfig): The data quality configuration containing metadata such as dataset name, ID, measurement sample, and lifecycle stage.
        dq_result (DataQualityResult): The data quality result to which metadata will be copied.

    Returns:
        DataQualityResult: A new instance of DataQualityResult with updated metadata fields.
    """

    dq_result_new = dq_result.model_copy()
    dq_result_new.dataset_name = config.dataset_name
    dq_result_new.dataset_id = config.dataset_id
    dq_result_new.measurement_sample = config.measurement_sample
    dq_result_new.lifecycle_stage = config.lifecycle_stage
    if config.measurement_time is not None:
        # we override 'UTC now' if measurement_time is specified in the config
        dq_result_new.measurement_time = config.measurement_time

    return dq_result_new


def _load_data_quality_config_files(
    file_paths: str | Path | list[str] | list[Path],
    regex_yaml_path: str | Path | None = None,
) -> DataQualityConfig:
    r"""
    Loads data quality configuration files, optionally replacing regular expression patterns from a provided YAML file.

    Args:
        file_paths: Single path or list of paths to configuration file(s) to load.
        regex_yaml_path: Optional path to a YAML file containing regular expression patterns to replace in configuration(s).
            The YAML file must contain key-value pairs such as 'EMAIL_REGEX': '<regex_here>'.
            Use single quotes around the regex value within the YAML file (rather than double quotes or no quotes), this let's
            you use typical regex escape characters like \d for a digit.
            Each matching key will have its value substituted within the data quality configuration(s). Defaults to None.

    Returns:
        The loaded data quality configuration. If multiple files are provided, rules from all configurations are combined,
        while other parameters (such as 'dataset_name' and 'measurement_time') are taken only from the first file.

    Raises:
        FileNotFoundError: If any specified configuration file does not exist.

    Warnings:
        If multiple configuration files are provided, only the 'rules' from subsequent files are merged.
          Items such as 'dataset_name' and 'measurement_time' (and any other non-rule parameters) are taken
          exclusively from the first file.

    """

    if isinstance(file_paths, str | Path):
        list_of_file_paths = [file_paths]
    else:
        list_of_file_paths = file_paths

    for fp in list_of_file_paths:
        if not Path(fp).exists():
            raise FileNotFoundError(f"Config file not found: {fp}")

    # Load regex patterns if path provided
    regex_patterns = {}
    if regex_yaml_path:
        regex_patterns = _load_regex_yaml(regex_yaml_path)

    # Load and validate configs
    configs = []
    for fp in list_of_file_paths:
        with open(fp) as f:
            cfg = yaml.safe_load(f)
            validated_cfg = DataQualityConfig(**cfg)

            validated_cfg = _replace_regex_values(validated_cfg, regex_patterns)
            configs.append(validated_cfg)

    # Merge configs
    if len(configs) == 1:
        return configs[0]
    else:
        # Warn user about merging specifics
        first_file_path = str(list_of_file_paths[0])
        warnings.warn(
            f"Multiple configuration files loaded. Only 'rules' from gchq_data_quality.configs will be merged. "
            f"Items like 'dataset_name' and 'measurement_time' are only taken from the first file: {first_file_path}.",
            stacklevel=2,
        )

        # Merge rules from all configs
        combined = configs[0].model_copy()
        for cfg in configs[1:]:
            combined.rules.extend(cfg.rules)
        return combined


def _load_regex_yaml(file_path: str | Path) -> dict[str, str]:
    r"""
    Load regular expression patterns from a YAML file and validate that all values are strings.

    Args:
        file_path (str or Path): Path to the YAML file containing regular expression patterns.

    Returns:
        dict: A dictionary mapping pattern names to regular expression strings.

    Raises:
        ValueError: If the YAML does not contain a mapping of key-value pairs, or any value is not a string.

    Example:
        The YAML file should look like:
            ```yaml
            PET_NAME_REGEX: '^[A-Za-z]{2,20}$'
            FOOD_WEIGHT_REGEX: '^\d+(\.\d{1,2})?\s?(kg|g|lb|oz)$'
            ```
    """
    with open(file_path) as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        raise ValueError(
            f"Regex YAML file should contain a dictionary of key-value pairs. You have type: {type(data)}. The format should be like this: REGEX_KEY: '<regex_value_here>'"
        )

    for key, value in data.items():
        if not isinstance(value, str):
            raise ValueError(
                f"Value for key '{key}' ({value}) is not a string. It is {type(value)}"
            )

    return data


def _replace_regex_values(
    config: DataQualityConfig, regex_dict: dict
) -> DataQualityConfig:
    """
    Substitute the 'regex_pattern' attribute for rules using the 'validity_regex' function,
    replacing named patterns with strings from a provided mapping.

    Args:
        config (DataQualityConfig): The data quality configuration object.
        regex_dict (dict): Dictionary mapping pattern names to regular expression strings (e.g., from a YAML file).

    Returns:
        DataQualityConfig: A new configuration object with updated regex patterns.

    Note:
        This function is generally for internal use within configuration loaders.
    """
    # Create a copy of the config
    updated_config = config.model_copy()

    for rule in updated_config.rules:
        if isinstance(rule, ValidityRegexRule):
            pattern_value = rule.regex_pattern
            if pattern_value and pattern_value in regex_dict:
                rule.regex_pattern = regex_dict[pattern_value]
    return updated_config


def _get_config_values_from_first_record(report: DataQualityReport) -> dict:
    """Returns a dictionary of the metadata for the DataQualityConfig class based off the first record.
    We can't aggregate or concatenate these, only one set per config object. e.g. if the DataQualityReport had
    two different 'measurement_sample' strings, we have to decide which one to pick for the DataQualityConfig -
    we pick the first one we find be default."""
    first_record = report.results[0]

    fields_to_populate = list(DataQualityConfig.model_fields.keys())
    fields_to_populate.remove(
        "rules"
    )  # we populate rules based on all records, not just the first

    config_kwargs = {key: getattr(first_record, key) for key in fields_to_populate}

    # Warn if inconsistent metadata
    for key in fields_to_populate:
        values = {getattr(entry, key) for entry in report.results}
        if len(values) > 1:  # there are different values
            warnings.warn(
                f"Inconsistent values for '{key}' in DataQualityReport: {values}. "
                f"Using '{config_kwargs[key]}' from the first entry.",
                stacklevel=2,
            )
    return config_kwargs
