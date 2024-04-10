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
    assert "#model" == next_line()
    assert "#dbt" == next_line()

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
    assert "#model" == next_line()
    assert "#dbt" == next_line()

    assert not next_line()  # end


def test_convert_gets_correct_tags(contexts):
    model_type_context = contexts[1]
    test_type_contest = contexts[3]
    source_type_context = contexts[5]
    project_context = contexts[7]

    assert "#test" in test_type_contest.data["description"]
    assert "#model" in model_type_context.data["description"]
    assert "#source" in source_type_context.data["description"]
    assert all(
        tag in project_context.data["description"]
        for tag in ["#test", "#model", "#source"]
    )
