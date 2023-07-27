import abc
import json
import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, List

import structlog

log = structlog.get_logger()


class ConvertFormat(str, Enum):
    MERMAID = "mermaid"
    MERMAID_10 = "mermaid@10"


@dataclass
class ConversionResult:
    table_name: str
    table_data: dict
    data: Any
    path: Path = None


class Converter(abc.ABC):
    def __init__(self):
        self.source_dir = os.getenv(
            "DBT_ARTIFACTS_SOURCE_DIR",
            ".meltano/transformers/dbt/target",
        )
        self.output_dir = os.getenv(
            "DBT_ARTIFACTS_OUTPUT_DIR",
            "output",
        )

        log.debug(f"Source directory: {self.source_dir}")
        log.debug(f"Output directory: {self.output_dir}")

    @abc.abstractmethod
    def convert(self) -> List[ConversionResult]:
        return []

    @abc.abstractmethod
    def write(self, path, data):
        return Path(path)

    def run(self):
        log.info(f"Using {self.__class__.__name__}")

        self.load_artifacts()
        self.convert()

    def load_artifacts(self):
        with open(f"{self.source_dir}/manifest.json") as manifest_file:
            self.manifest: dict = json.load(manifest_file)

        with open(f"{self.source_dir}/catalog.json") as catalog_file:
            self.catalog: dict = json.load(catalog_file)

        return self.manifest, self.catalog
