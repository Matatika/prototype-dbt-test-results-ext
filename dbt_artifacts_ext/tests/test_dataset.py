import pytest

from dbt_artifacts_ext.converter.matatika import MatatikaConverter


@pytest.fixture()
def contexts():
    converter = MatatikaConverter()
    converter.load_artifacts()

    return converter.convert()


def test_convert_with_model_and_column_descriptions(contexts):
    model_type_context = contexts[0]
    description: str = model_type_context.data["description"]
    lines = iter(description.splitlines())

    def next_line():
        return next(lines, None)

    assert "Test dbt model 1" == next_line()
    assert "" == next_line()
    assert "| Column | Description |" == next_line()
    assert "| --- | --- |" == next_line()
    assert "| `model_column_1` | Description for `model_column_1` |" == next_line()
    assert "| `model_column_2` | Description for `model_column_2` |" == next_line()
    assert "| `model_column_3` | Description for `model_column_3` |" == next_line()
    assert "" == next_line()
    assert "#model #dbt" == next_line()

    assert not next_line()  # end


def test_convert_without_model_and_column_descriptions(contexts):
    model_type_context_undocumented = contexts[1]
    description: str = model_type_context_undocumented.data["description"]
    lines = iter(description.splitlines())

    def next_line():
        return next(lines, None)

    assert "| Column | Description |" == next_line()
    assert "| --- | --- |" == next_line()
    assert "| `model_column_1` |  |" == next_line()
    assert "| `model_column_2` |  |" == next_line()
    assert "| `model_column_3` |  |" == next_line()
    assert "" == next_line()
    assert "#model #dbt" == next_line()

    assert not next_line()  # end


def test_convert_gets_correct_tags(contexts):
    model_type_context = contexts[1]
    test_type_context = contexts[3]
    source_type_context = contexts[5]
    project_context = contexts[7]

    assert "#test #dbt" in test_type_context.data["description"]
    assert "#model #dbt" in model_type_context.data["description"]
    assert "#source #dbt" in source_type_context.data["description"]
    assert "#source #model #snapshot #test #dbt" in project_context.data["description"]
