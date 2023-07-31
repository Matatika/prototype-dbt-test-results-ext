import json

import structlog
import yaml
from matatika.dataset import DatasetV0_2

from dbt_artifacts_ext.converter import ConversionContext, Converter
from dbt_artifacts_ext.converter.mermaid import INDENT, MermaidConverter

DEFAULT_SOURCE = "dbt models"

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
            dataset = DatasetV0_2()
            dataset.title = result.metadata["name"]
            dataset.description = result.metadata["description"] or None
            dataset.source = DEFAULT_SOURCE
            dataset.visualisation = json.dumps({"mermaid": {}}, indent=INDENT)
            dataset.raw_data = result.data

            result.data = dataset.to_dict(apply_translations=False)

        return results

    def write(self, result: ConversionContext):
        path = Converter.write(self, result)

        with open(path, "w") as f:
            yaml.dump(
                result.data,
                f,
                indent=len(INDENT),
                width=float("inf"),
            )
