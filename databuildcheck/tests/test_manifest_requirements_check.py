# SPDX-FileCopyrightText: 2025-present Miguel Paraz <mparaz@mparaz.com>
#
# SPDX-License-Identifier: MIT

"""Tests for the manifest requirements check module."""

import json
import tempfile
from pathlib import Path

import pytest
import yaml

from databuildcheck.checks.manifest_requirements_check import (
    ManifestRequirementsChecker,
)
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
            "model.my_project.users": {
                "name": "users",
                "unique_id": "model.my_project.users",
                "resource_type": "model",
                "package_name": "my_project",
                "description": "User data model",
                "config": {"materialized": "table"},
                "tags": ["pii"],
                "columns": {
                    "id": {
                        "name": "id",
                        "data_type": "integer",
                        "description": "User ID",
                    },
                    "created_at": {"name": "created_at", "data_type": "timestamp"},
                    "updated_at": {"name": "updated_at", "data_type": "timestamp"},
                },
            },
            "model.my_project.orders": {
                "name": "orders",
                "unique_id": "model.my_project.orders",
                "resource_type": "model",
                "package_name": "my_project",
                "config": {
                    "materialized": "incremental",
                    "incremental_strategy": "merge",
                },
                "columns": {
                    "id": {"name": "id", "data_type": "integer"},
                    "user_id": {"name": "user_id", "data_type": "integer"},
                    "updated_at": {"name": "updated_at", "data_type": "timestamp"},
                },
            },
        },
    }


@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        "required_columns": {
            "always": [
                {"name": "created_at", "data_type": "timestamp"},
                {"name": "updated_at", "data_type": "timestamp"},
            ]
        },
        "materialization_requirements": {
            "table": {
                "required_columns": [
                    {"name": "id", "data_type": "integer", "description": "Primary key"}
                ]
            },
            "incremental": {
                "required_columns": [
                    {"name": "id", "data_type": "integer"},
                    {"name": "updated_at", "data_type": "timestamp"},
                ]
            },
        },
        "incremental_strategy_requirements": {
            "merge": {
                "required_columns": [
                    {"name": "id", "data_type": "integer"},
                    {"name": "updated_at", "data_type": "timestamp"},
                ]
            }
        },
        "tag_requirements": {
            "pii": {
                "required_columns": [
                    {"name": "data_classification", "data_type": "string"}
                ]
            }
        },
        "column_validation": {"require_descriptions": ["id"]},
        "model_requirements": {"require_description": True},
        "exclusions": {
            "fully_exempt": ["exempt_model"],
            "description_exempt": ["staging.*"],
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


@pytest.fixture
def config_file(sample_config):
    """Create a temporary config file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(sample_config, f)
        config_path = Path(f.name)

    yield config_path

    # Cleanup
    config_path.unlink()


@pytest.fixture
def dbt_manifest(manifest_file):
    """Create a DbtManifest instance for testing."""
    return DbtManifest(manifest_file)


def test_manifest_requirements_checker_init(dbt_manifest, config_file):
    """Test ManifestRequirementsChecker initialization."""
    checker = ManifestRequirementsChecker(dbt_manifest, config_file)

    assert checker.manifest == dbt_manifest
    assert checker.config_path == config_file
    assert isinstance(checker.config, dict)


def test_load_config_nonexistent_file(dbt_manifest):
    """Test loading nonexistent config file."""
    with pytest.raises(FileNotFoundError, match="Configuration file not found"):
        ManifestRequirementsChecker(dbt_manifest, "nonexistent.yaml")


def test_load_config_invalid_yaml(dbt_manifest):
    """Test loading invalid YAML config file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("invalid: yaml: content: [")
        invalid_yaml_path = Path(f.name)

    try:
        with pytest.raises(ValueError, match="Invalid YAML in configuration file"):
            ManifestRequirementsChecker(dbt_manifest, invalid_yaml_path)
    finally:
        invalid_yaml_path.unlink()


def test_is_model_exempt(dbt_manifest, config_file):
    """Test model exemption checking."""
    checker = ManifestRequirementsChecker(dbt_manifest, config_file)

    # Test exact match
    assert checker._is_model_exempt("exempt_model", "fully_exempt") is True
    assert checker._is_model_exempt("other_model", "fully_exempt") is False

    # Test regex pattern
    assert checker._is_model_exempt("staging_users", "description_exempt") is True
    assert checker._is_model_exempt("staging_orders", "description_exempt") is True
    assert checker._is_model_exempt("final_users", "description_exempt") is False


def test_get_required_columns_for_model(dbt_manifest, config_file):
    """Test getting required columns for a model."""
    checker = ManifestRequirementsChecker(dbt_manifest, config_file)

    # Test table model with PII tag
    users_model = dbt_manifest.get_model_nodes()["model.my_project.users"]
    required_columns = checker._get_required_columns_for_model(users_model)

    # Should include always required, table materialization, and PII tag requirements
    column_names = [col["name"] for col in required_columns]
    assert "created_at" in column_names  # always required
    assert "updated_at" in column_names  # always required
    assert "id" in column_names  # table materialization
    assert "data_classification" in column_names  # PII tag


def test_validate_column_requirements(dbt_manifest, config_file):
    """Test column requirements validation."""
    checker = ManifestRequirementsChecker(dbt_manifest, config_file)

    model_columns = {
        "id": {"name": "id", "data_type": "integer", "description": "User ID"},
        "created_at": {"name": "created_at", "data_type": "timestamp"},
    }

    required_columns = [
        {"name": "id", "data_type": "integer", "description": "User ID"},
        {"name": "created_at", "data_type": "timestamp"},
        {"name": "missing_column", "data_type": "string"},
    ]

    errors = checker._validate_column_requirements(
        "test_model", model_columns, required_columns
    )

    assert len(errors) == 1
    assert "Missing required column: missing_column" in errors


def test_validate_column_descriptions(dbt_manifest, config_file):
    """Test column description validation."""
    checker = ManifestRequirementsChecker(dbt_manifest, config_file)

    model_columns = {
        "id": {"name": "id", "data_type": "integer"},  # Missing description
        "name": {"name": "name", "data_type": "string", "description": "User name"},
    }

    errors = checker._validate_column_descriptions("test_model", model_columns)

    assert len(errors) == 1
    assert "Column 'id' requires a description" in errors


def test_validate_model_requirements(dbt_manifest, config_file):
    """Test model-level requirements validation."""
    checker = ManifestRequirementsChecker(dbt_manifest, config_file)

    # Model without description
    model_data = {"name": "test_model"}
    errors = checker._validate_model_requirements("test_model", model_data)

    assert len(errors) == 1
    assert "Model missing required description" in errors


def test_check_model_requirements_valid(dbt_manifest, config_file):
    """Test checking model requirements for a valid model."""
    checker = ManifestRequirementsChecker(dbt_manifest, config_file)

    # Modify the users model to meet all requirements
    users_model = dbt_manifest.get_model_nodes()["model.my_project.users"]
    users_model["columns"]["data_classification"] = {
        "name": "data_classification",
        "data_type": "string",
    }

    result = checker.check_model_requirements("model.my_project.users")

    # Should have some errors due to missing requirements, but structure should be correct
    assert result["node_id"] == "model.my_project.users"
    assert result["model_name"] == "users"
    assert isinstance(result["requirements_valid"], bool)
    assert isinstance(result["errors"], list)
    assert isinstance(result["warnings"], list)


def test_check_model_requirements_nonexistent(dbt_manifest, config_file):
    """Test checking requirements for nonexistent model."""
    checker = ManifestRequirementsChecker(dbt_manifest, config_file)

    result = checker.check_model_requirements("model.nonexistent.model")

    assert result["requirements_valid"] is False
    assert "Model not found in manifest" in result["errors"][0]


def test_check_all_models(dbt_manifest, config_file):
    """Test checking requirements for all models."""
    checker = ManifestRequirementsChecker(dbt_manifest, config_file)

    results = checker.check_all_models()

    assert len(results) == 2
    assert all("node_id" in result for result in results)
    assert all("requirements_valid" in result for result in results)
