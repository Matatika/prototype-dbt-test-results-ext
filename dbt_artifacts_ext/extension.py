"""Meltano DbtArtifacts extension."""
from __future__ import annotations

import structlog
from meltano.edk import models
from meltano.edk.extension import ExtensionBase

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
