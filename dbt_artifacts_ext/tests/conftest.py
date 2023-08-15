import os
import shutil
from pathlib import Path

import pytest

from dbt_artifacts_ext.converter import Converter
from dbt_artifacts_ext.converter.matatika import MatatikaConverter
from dbt_artifacts_ext.converter.mermaid import MermaidConverter
from dbt_artifacts_ext.tests import root


@pytest.fixture(autouse=True)
def test_dir(tmp_path: Path):
    os.chdir(tmp_path)

    return tmp_path


@pytest.fixture(autouse=True)
def target_dir(test_dir: Path):
    target_dir = test_dir / ".meltano" / "transformers" / "dbt" / "target"
    target_dir.mkdir(parents=True)

    return target_dir


@pytest.fixture(autouse=True)
def output_dir(test_dir: Path):
    output_dir = test_dir / "output"
    output_dir.mkdir()

    return output_dir


@pytest.fixture(autouse=True)
def manifest(target_dir: Path):
    return shutil.copy(root / "manifest.json", target_dir)


@pytest.fixture(autouse=True)
def catalog(target_dir: Path):
    return shutil.copy(root / "catalog.json", target_dir)


@pytest.fixture(params=[MermaidConverter, MatatikaConverter])
def converter(request: pytest.FixtureRequest):
    converter: Converter = request.param()
    converter.load_artifacts()

    return converter
