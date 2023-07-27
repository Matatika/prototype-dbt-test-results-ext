import abc
import json
import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any

import structlog

log = structlog.get_logger()


class ConvertFormat(str, Enum):
    MERMAID = "mermaid"
    MERMAID_10 = "mermaid@10"


@dataclass
class ConversionContext:
    metadata: dict
    data: Any


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

    @property
    def file_ext(self) -> str:
        ...

    @abc.abstractmethod
    def convert(self) -> list[ConversionContext]:
        return []

    @abc.abstractmethod
    def write(self, result: ConversionContext):
        file_ext = self.file_ext

        if not file_ext.startswith("."):
            file_ext = f".{file_ext}"

        unique_id = result.metadata["unique_id"]
        return Path(f"{self.output_dir}/{unique_id}{file_ext}")

    def run(self):
        log.info(f"Using {self.__class__.__name__}")

        self.load_artifacts()
        results = self.convert()

        for result in results:
            self.write(result)

    def load_artifacts(self):
        with open(f"{self.source_dir}/manifest.json") as manifest_file:
            self.manifest: dict = json.load(manifest_file)

        with open(f"{self.source_dir}/catalog.json") as catalog_file:
            self.catalog: dict = json.load(catalog_file)

        return self.manifest, self.catalog
