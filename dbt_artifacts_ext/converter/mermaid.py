import re

import structlog

from dbt_artifacts_ext.converter import ConversionContext, Converter

INDENT = " " * 4
NODE_CONNECTOR = "}|--|{"
RESOURCE_TYPES = {
    "source",
    "model",
    "snapshot",
}

log = structlog.get_logger()


class MermaidConverter(Converter):
    file_ext = ".mmd"

    def convert(self):
        results = super().convert()

        parent_map: dict = self.manifest["parent_map"]
        child_map: dict = self.manifest["child_map"]

        tables: dict[str, dict] = {
            **self.catalog["nodes"],
            **self.catalog["sources"],
        }

        metadata: dict[str, dict] = {
            **self.manifest["nodes"],
            **self.manifest["sources"],
        }

        for i, (table_name, table_data) in enumerate(metadata.items()):
            progress = f"[{i + 1}/{len(metadata)}]\t"
            resource_type = table_data["resource_type"]

            if resource_type not in RESOURCE_TYPES:
                log.info(f"{progress}Skipping {resource_type} '{table_name}'")
                continue

            log.info(f"{progress}Processing {resource_type} '{table_name}'")

            lines = [
                "erDiagram",
                f'{INDENT}"{table_name}" {{',
            ]

            columns: dict[str, dict] = tables.get(table_name, {}).get("columns", {})

            for column_name, column_data in columns.items():
                column_name = re.sub(r"[^\w\(\)\[\]]", "_", column_name)
                column_type = column_data["type"].replace(" ", "_")

                lines.append(f"{INDENT * 2}{column_type} {column_name}")

            lines.append(f"{INDENT}}}")
            lines.append("")

            related_tables: dict[str, list[str]] = {
                "upstream": parent_map[table_name],
                "downstream": child_map[table_name],
            }

            for rel_name, related_table_names in related_tables.items():
                if not related_table_names:
                    continue

                lines.append(f"{INDENT}%% {rel_name} relationships")

                for related_table_name in related_table_names:
                    source, destination = (
                        (related_table_name, table_name)
                        if rel_name == "upstream"
                        else (table_name, related_table_name)
                    )

                    lines.append(
                        f'{INDENT}"{source}" {NODE_CONNECTOR} "{destination}" : ""'
                    )

            results.append(ConversionContext(table_data, "\n".join(lines)))

        return results

    def write(self, result: ConversionContext):
        path = super().write(result)

        with open(path, "w") as f:
            f.write(result.data)
