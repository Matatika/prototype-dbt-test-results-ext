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
    MATATIKA = "matatika"
    MATATIKA_0 = "matatika@0"


class ResourceType(str, Enum):
    SOURCE = "source"
    MODEL = "model"
    SNAPSHOT = "snapshot"
    TEST = "test"
    ALL = "all"


@dataclass
class ConversionContext:
    identifier: Any
    metadata: dict
    description: "list[str]"
    tags: "list[str]"
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

        resource_types = os.getenv("DBT_ARTIFACTS_RESOURCE_TYPES")
        exclude_packages = os.getenv("DBT_ARTIFACTS_EXCLUDE_PACKAGES")

        self.resource_types = (
            [ResourceType(rt) for rt in json.loads(resource_types)]
            if resource_types
            else [ResourceType.ALL]
        )

        self.exclude_packages = json.loads(exclude_packages) if exclude_packages else []

        if ResourceType.ALL in self.resource_types:
            self.resource_types = [
                rt for rt in ResourceType if rt is not ResourceType.ALL
            ]

        log.debug(f"Resource types: {self.resource_types}")
        log.debug(f"Exclude packages: {self.exclude_packages}")

    @property
    @abc.abstractmethod
    def file_ext(self) -> str: ...

    @abc.abstractmethod
    def convert(self) -> List[ConversionContext]:
        return []

    @abc.abstractmethod
    def write(self, result: ConversionContext):
        file_ext = self.file_ext

        if not file_ext.startswith("."):
            file_ext = f".{file_ext}"

        return Path(f"{self.output_dir}/{result.identifier}{file_ext}")

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
