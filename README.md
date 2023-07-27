# DbtArtifacts

DbtArtifacts is A Meltano utility extension for processing [dbt artifacts](https://docs.getdbt.com/reference/artifacts/dbt-artifacts).

## Installing this extension for local development

1. Install the project dependencies with `poetry install`:

```shell
cd path/to/your/project
poetry install
```

2. Verify that you can invoke the extension:

```shell
poetry run dbt-artifacts_extension --help
poetry run dbt-artifacts_extension describe --format=yaml
```
