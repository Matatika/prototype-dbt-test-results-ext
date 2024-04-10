import json

import structlog
import yaml
from matatika.dataset import DatasetV0_2

from dbt_artifacts_ext.converter import ConversionContext, Converter
from dbt_artifacts_ext.converter.mermaid import INDENT, MermaidConverter

DEFAULT_SOURCE = "dbt models"
KEY_ORDER = ("version",)

log = structlog.get_logger()


def multiline_string_representer(dumper: yaml.Dumper, data: str):
    if "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


yaml.add_representer(str, multiline_string_representer)


class MatatikaConverter(MermaidConverter):
    file_ext = ".yml"

    def convert(self):
        results = super().convert()

        for result in results:
            md_table = [
                "| Column | Description |",
                "| --- | --- |",
                *result.md_table_rows,
            ]

            tags = [f"#{t}" for t in (*result.tags, "dbt") if t]

            description_parts = ["\n".join(s) for s in (md_table, tags)]

            if "description" in result.metadata:
                description_parts.insert(0, result.metadata["description"])

            dataset = DatasetV0_2()
            dataset.title = result.metadata.get("name") or result.identifier
            dataset.description = "\n\n".join(description_parts)
            dataset.source = DEFAULT_SOURCE
            dataset.visualisation = json.dumps({"mermaid": {}}, indent=INDENT)
            dataset.raw_data = result.data

            result.data = {
                **dict.fromkeys(KEY_ORDER),
                **dataset.to_dict(apply_translations=False),
            }

        return results

    def write(self, result: ConversionContext):
        path = Converter.write(self, result)

        with open(path, "w") as f:
            yaml.dump(
                result.data,
                f,
                indent=len(INDENT),
                width=float("inf"),
                sort_keys=False,
            )
