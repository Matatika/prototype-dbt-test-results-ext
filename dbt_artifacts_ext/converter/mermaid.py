import re

import structlog

from dbt_artifacts_ext.converter import ConversionContext, Converter

INDENT = " " * 4
NODE_CONNECTOR = "}|--|{"

log = structlog.get_logger()


class MermaidConverter(Converter):
    file_ext = ".mmd"

    def convert(self):
        results = super().convert()

        parent_map: dict[str, list] = self.manifest["parent_map"]
        child_map: dict[str, list] = self.manifest["child_map"]

        tables: dict[str, dict] = {
            **self.catalog["nodes"],
            **self.catalog["sources"],
        }

        metadata: dict[str, dict] = {
            **self.manifest["nodes"],
            **self.manifest["sources"],
        }

        package_metadata = {
            package_name: {
                k: v for k, v in metadata.items() if v["package_name"] == package_name
            }
            for package_name in [v["package_name"] for v in metadata.values()]
        }

        for package_name, metadata in package_metadata.items():
            log.info(f"Processing package '{package_name}'")

            total_resources = len(metadata)
            full_lines = []

            for i, (table_name, table_data) in enumerate(metadata.items()):
                current = str(i + 1).rjust(len(str(total_resources)))
                progress = f"[{current}/{total_resources}]\t"
                resource_type = table_data["resource_type"]
                package_name = table_data["package_name"]

                if (
                    resource_type not in self.resource_types
                    or package_name in self.exclude_packages
                ):
                    log.info(f"{progress}Skipping {resource_type} '{table_name}'")
                    continue

                log.info(f"{progress}Processing {resource_type} '{table_name}'")

                lines = [
                    "erDiagram",
                    f'{INDENT}"{table_name}" {{',
                ]

                columns: dict[str, dict] = tables.get(table_name, {}).get("columns", {})

                description: list = []

                for column_name, column_data in columns.items():
                    description_table_row = f"| {column_name} |"

                    column_name_underscored = re.sub(r"[^\w\(\)\[\]]", "_", column_name)
                    column_type = column_data["type"].replace(" ", "_")
                    column_metadata = metadata.get(table_name, {}).get("columns", {})

                    if any(column_name == key for key in column_metadata.keys()):
                        column_description = column_metadata[column_name]["description"]
                        description_table_row += f" {column_description} |"
                        column_description = f'"{column_description}"'
                    else:
                        column_description = ""
                        description_table_row += " |"

                    description.append(description_table_row)

                    lines.append(
                        f"{INDENT * 2}{column_name_underscored} {column_type} {column_description}"
                    )

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

                results.append(
                    ConversionContext(
                        table_data["unique_id"],
                        table_data,
                        description,
                        [resource_type],
                        "\n".join(lines),
                    )
                )

                for parent_name in parent_map[table_name]:
                    full_lines.append(
                        f'{INDENT}"{parent_name}" {NODE_CONNECTOR} "{table_name}" : ""'
                    )

            if not full_lines:
                continue

            full_lines.insert(0, "erDiagram")
            results.append(
                ConversionContext(
                    package_name,
                    metadata,
                    description,
                    self.resource_types,
                    "\n".join(full_lines),
                )
            )

        return results

    def write(self, result: ConversionContext):
        path = super().write(result)

        with open(path, "w") as f:
            f.write(result.data)
