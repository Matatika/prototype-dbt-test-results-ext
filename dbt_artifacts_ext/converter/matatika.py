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
            description_parts = [result.metadata.get("description")]
            if result.description:
                result_description = ["| Column | Description |", "| --- | --- |"]
                result_description.extend(result.description)
                description_parts.append("\n".join(result_description))

            result.tags.append("dbt")

            dataset = DatasetV0_2()
            dataset.title = result.metadata.get("name") or result.identifier
            dataset.description = "\n\n".join(p for p in description_parts if p)
            dataset.description += "\n\n" + " ".join(f"#{t}" for t in result.tags if t)
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
