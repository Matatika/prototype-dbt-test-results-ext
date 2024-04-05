from dbt_artifacts_ext.converter.matatika import MatatikaConverter


def test_convert_with_model_and_column_descriptions():
    converter = MatatikaConverter()
    converter.load_artifacts()

    contexts = converter.convert()
    context_with_descriptions = contexts[0]

    dataset_description: str = context_with_descriptions.data["description"]

    assert dataset_description.startswith("Test dbt model 1")
    assert "| Column |" in dataset_description
    assert "| Description |" in dataset_description
    assert "| --- |" in dataset_description
    assert "| model_column_1 |" in dataset_description
    assert "| model_column_2 |" in dataset_description
    assert "| model_column_3 |" in dataset_description

    assert (
        "This is my column_one description"
        in context_with_descriptions.metadata["columns"]["column_one"]["description"]
    )


def test_convert_without_model_and_column_descriptions():
    converter = MatatikaConverter()
    converter.load_artifacts()

    contexts = converter.convert()
    context_without_description = contexts[1]

    dataset_description: str = context_without_description.data["description"]

    assert dataset_description.startswith("| Column |")


def test_convert_gets_correct_tags():
    converter = MatatikaConverter()
    converter.load_artifacts()

    contexts = converter.convert()
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
