import json
import re
from textwrap import dedent

import structlog
import yaml
from matatika.dataset import DatasetV0_2

from dbt_artifacts_ext.converter import ConversionContext, Converter, ResourceType
from dbt_artifacts_ext.converter.mermaid import INDENT

DEFAULT_SOURCE = "Test Results"
KEY_ORDER = ("version",)

log = structlog.get_logger()


def multiline_string_representer(dumper: yaml.Dumper, data: str):
    if "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


yaml.add_representer(str, multiline_string_representer)


class MatatikaConverter(Converter):
    file_ext = ".yml"

    def convert(self):
        results = super().convert()

        nodes: dict[str, dict] = self.manifest["nodes"]

        test_nodes: list[dict[str, str]] = [
            node
            for node in nodes.values()
            if node["resource_type"] == ResourceType.TEST
        ]

        for test_node in test_nodes:
            dataset = DatasetV0_2()
            dataset.title = re.sub(r"[\W_]", " ", test_node["name"]).capitalize()
            dataset.description = "\n\n".join(
                text
                for text in [
                    test_node.get("description"),
                    "#test_results",
                ]
                if text
            )
            dataset.source = DEFAULT_SOURCE

            dataset.metadata = json.dumps(
                {
                    "name": "TEST_FAILURE_CENTRAL",
                    "related_table": {
                        "columns": [
                            {
                                "name": "TEST_FAILURES_JSON",
                                "label": "Test failures JSON",
                                "post_process": "json_parse",
                            },
                        ]
                    },
                },
                indent=INDENT,
            )

            dataset.visualisation = json.dumps(
                {"html-table": {}},
                indent=INDENT,
            )

            dataset.query = dedent(
                f"""
                SELECT TEST_FAILURES_JSON
                FROM MATATIKA_TEST_RESULTS.TEST_FAILURE_CENTRAL
                WHERE TEST_NAME = '{test_node["unique_id"]}'
                AND TEST_RUN_TIME = (
                    SELECT MAX(TEST_RUN_TIME)
                    FROM MATATIKA_TEST_RESULTS.TEST_FAILURE_CENTRAL
                    WHERE TEST_NAME = '{test_node["unique_id"]}'
                )
                """
            ).strip("\n")

            results.append(
                ConversionContext(
                    test_node["unique_id"],
                    {},
                    {
                        **dict.fromkeys(KEY_ORDER),
                        **dataset.to_dict(apply_translations=False),
                    },
                )
            )

        return results

    def write(self, result):
        path = super().write(result)

        with open(path, "w") as f:
            yaml.dump(
                result.data,
                f,
                indent=len(INDENT),
                width=float("inf"),
                sort_keys=False,
            )
