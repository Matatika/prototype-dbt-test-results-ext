import os

from dbt_artifacts_ext.converter import Converter


def test_run(converter: Converter):
    converter.run()

    output = os.listdir(converter.output_dir)

    assert f"source.test_project.source_table_1{converter.file_ext}" in output
    assert f"source.test_project.source_table_2{converter.file_ext}" in output
    assert f"source.test_project.source_table_3{converter.file_ext}" in output

    assert f"model.test_project.model_table_1{converter.file_ext}" in output
    assert f"model.test_project.model_table_2{converter.file_ext}" in output

    assert f"test.test_project.test_table_1{converter.file_ext}" in output
    assert f"test.test_project.test_table_2{converter.file_ext}" in output

    assert f"test_project{converter.file_ext}" in output


def test_run_exclude_packages(converter: Converter):
    converter.exclude_packages = ["test_project"]
    converter.run()

    output = os.listdir(converter.output_dir)

    assert f"source.test_project.source_table_1{converter.file_ext}" not in output
    assert f"source.test_project.source_table_2{converter.file_ext}" not in output
    assert f"source.test_project.source_table_3{converter.file_ext}" not in output

    assert f"model.test_project.model_table_1{converter.file_ext}" not in output
    assert f"model.test_project.model_table_2{converter.file_ext}" not in output

    assert f"test.test_project.test_table_1{converter.file_ext}" not in output
    assert f"test.test_project.test_table_2{converter.file_ext}" not in output

    assert f"test_project{converter.file_ext}" not in output
