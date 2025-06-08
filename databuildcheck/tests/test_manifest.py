# SPDX-FileCopyrightText: 2025-present Miguel Paraz <mparaz@mparaz.com>
#
# SPDX-License-Identifier: MIT

"""Tests for the manifest module."""

import json
import tempfile
from pathlib import Path

import pytest

from databuildcheck.manifest import DbtManifest


@pytest.fixture
def sample_manifest_data():
    """Sample manifest data for testing."""
    return {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12/index.html",
            "dbt_version": "1.8.0",
        },
        "nodes": {
            "model.my_project.my_model": {
                "name": "my_model",
                "unique_id": "model.my_project.my_model",
                "resource_type": "model",
                "package_name": "my_project",
                "original_file_path": "models/my_model.sql",
                "columns": {
                    "id": {"name": "id", "data_type": "integer"},
                    "name": {"name": "name", "data_type": "varchar"},
                },
            },
            "model.my_project.another_model": {
                "name": "another_model",
                "unique_id": "model.my_project.another_model",
                "resource_type": "model",
                "package_name": "my_project",
                "original_file_path": "models/another_model.sql",
                "columns": {
                    "user_id": {"name": "user_id", "data_type": "integer"},
                    "email": {"name": "email", "data_type": "varchar"},
                },
            },
            "test.my_project.test_my_model": {
                "name": "test_my_model",
                "unique_id": "test.my_project.test_my_model",
                "resource_type": "test",
                "package_name": "my_project",
                "original_file_path": "tests/test_my_model.sql",
            },
        },
    }


@pytest.fixture
def manifest_file(sample_manifest_data):
    """Create a temporary manifest file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(sample_manifest_data, f)
        manifest_path = Path(f.name)

    yield manifest_path

    # Cleanup
    manifest_path.unlink()


def test_dbt_manifest_init_with_valid_file(manifest_file):
    """Test DbtManifest initialization with a valid file."""
    manifest = DbtManifest(manifest_file)
    assert manifest.manifest_path == manifest_file
    assert len(manifest.nodes) == 3


def test_dbt_manifest_init_with_nonexistent_file():
    """Test DbtManifest initialization with a nonexistent file."""
    with pytest.raises(FileNotFoundError, match="Manifest file not found"):
        DbtManifest("nonexistent.json")


def test_dbt_manifest_init_with_invalid_json():
    """Test DbtManifest initialization with invalid JSON."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        f.write("invalid json content")
        invalid_json_path = Path(f.name)

    try:
        with pytest.raises(ValueError, match="Invalid JSON in manifest file"):
            DbtManifest(invalid_json_path)
    finally:
        invalid_json_path.unlink()


def test_get_model_nodes(manifest_file):
    """Test getting model nodes from the manifest."""
    manifest = DbtManifest(manifest_file)
    model_nodes = manifest.get_model_nodes()

    assert len(model_nodes) == 2
    assert "model.my_project.my_model" in model_nodes
    assert "model.my_project.another_model" in model_nodes
    assert "test.my_project.test_my_model" not in model_nodes


def test_get_model_columns(manifest_file):
    """Test getting columns for a specific model."""
    manifest = DbtManifest(manifest_file)

    columns = manifest.get_model_columns("model.my_project.my_model")
    assert len(columns) == 2
    assert "id" in columns
    assert "name" in columns

    # Test with nonexistent model
    columns = manifest.get_model_columns("model.nonexistent.model")
    assert columns == {}


def test_get_model_original_file_path(manifest_file):
    """Test getting original file path for a specific model."""
    manifest = DbtManifest(manifest_file)

    path = manifest.get_model_original_file_path("model.my_project.my_model")
    assert path == "models/my_model.sql"

    # Test with nonexistent model
    path = manifest.get_model_original_file_path("model.nonexistent.model")
    assert path is None


def test_get_models_info(manifest_file):
    """Test getting information about all models."""
    manifest = DbtManifest(manifest_file)
    models_info = manifest.get_models_info()

    assert len(models_info) == 2

    # Check first model
    model1 = next(m for m in models_info if m["name"] == "my_model")
    assert model1["node_id"] == "model.my_project.my_model"
    assert model1["original_file_path"] == "models/my_model.sql"
    assert len(model1["columns"]) == 2
    assert model1["resource_type"] == "model"
    assert model1["package_name"] == "my_project"
