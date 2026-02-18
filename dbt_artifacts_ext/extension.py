"""Meltano DbtArtifacts extension."""

from __future__ import annotations

import structlog
from meltano.edk import models
from meltano.edk.extension import ExtensionBase

from dbt_artifacts_ext.converter import ConvertFormat
from dbt_artifacts_ext.converter.factory import ConverterFactory

log = structlog.get_logger()


class DbtArtifacts(ExtensionBase):
    """Extension implementing the ExtensionBase interface."""

    def invoke(self, *args):
        log.warning(f"{self.__class__.__name__} does not wrap a CLI")

    def describe(self) -> models.Describe:
        """Describe the extension.

        Returns:
            The extension description
        """
        # TODO: could we auto-generate all or portions of this from typer instead?
        return models.Describe(
            commands=[
                models.ExtensionCommand(
                    name="dbt-artifacts_extension", description="extension commands"
                ),
            ]
        )

    def convert(self, convert_format: ConvertFormat):
        converter = ConverterFactory.get(convert_format)
        converter.run()
