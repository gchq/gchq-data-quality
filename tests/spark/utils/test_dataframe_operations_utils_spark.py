# (c) Crown Copyright GCHQ \n
from gchq_data_quality.spark.dataframe_operations import (
    _extract_and_sort_nested_paths,
    _final_paths_from_df,
    _get_parent_column,
    _nested_paths_are_consistent,
    _rename_nested_paths_for_explosion,
)


def test_array_paths_are_valid(array_paths_are_valid_case: dict) -> None:
    result = _nested_paths_are_consistent(**array_paths_are_valid_case["inputs"])
    assert result == array_paths_are_valid_case["expected"]["is_valid"]


def test_rename_array_paths(rename_array_paths_case: dict) -> None:
    result = _rename_nested_paths_for_explosion(**rename_array_paths_case["inputs"])
    assert result == rename_array_paths_case["expected"]["renamed_paths"]


def test_final_paths_from_exploded_df(final_paths_from_exploded_df_case: dict) -> None:
    result = _final_paths_from_df(**final_paths_from_exploded_df_case["inputs"])
    assert result == final_paths_from_exploded_df_case["expected"]["final_paths"]


def test_get_parent_column(get_parent_column_case: dict) -> None:
    result = _get_parent_column(**get_parent_column_case["inputs"])
    assert result == get_parent_column_case["expected"]["parent_column"]


def test_extract_and_sort_array_paths(extract_and_sort_array_paths_case: dict) -> None:
    result = _extract_and_sort_nested_paths(
        **extract_and_sort_array_paths_case["inputs"]
    )

    assert result == extract_and_sort_array_paths_case["expected"]["array_paths"]
