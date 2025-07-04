# SPDX-FileCopyrightText: 2025-present Miguel Paraz <mparaz@mparaz.com>
#
# SPDX-License-Identifier: MIT

"""Tests for the CLI module."""

import json
import tempfile
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from databuildcheck.cli import main


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
                "original_file_path": "models/users.sql",
                "columns": {
                    "id": {"name": "id", "data_type": "integer"},
                    "name": {"name": "name", "data_type": "varchar"},
                    "email": {"name": "email", "data_type": "varchar"},
                },
            },
        },
        "sources": {
            "source.my_project.raw.raw_users": {
                "name": "raw_users",
                "unique_id": "source.my_project.raw.raw_users",
                "resource_type": "source",
                "package_name": "my_project",
                "database": "raw_db",
                "schema": "raw",
            },
        },
    }


@pytest.fixture
def test_files(sample_manifest_data):
    """Create temporary test files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        # Create manifest file
        manifest_file = temp_path / "manifest.json"
        with open(manifest_file, "w") as f:
            json.dump(sample_manifest_data, f)

        # Create SQL files directory
        sql_dir = temp_path / "compiled"
        sql_dir.mkdir()
        models_dir = sql_dir / "models"
        models_dir.mkdir()

        # Create SQL file
        sql_file = models_dir / "users.sql"
        sql_content = """
        SELECT
            id,
            name,
            email
        FROM raw_users
        """
        sql_file.write_text(sql_content)

        yield {
            "manifest": manifest_file,
            "sql_dir": sql_dir,
        }


def test_cli_help():
    """Test CLI help output."""
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])

    assert result.exit_code == 0
    assert "Check dbt models for consistency" in result.output
    assert "--manifest" in result.output
    assert "--compiled-sql" in result.output
    assert "--dialect" in result.output


def test_cli_missing_required_args():
    """Test CLI with missing required arguments."""
    runner = CliRunner()
    result = runner.invoke(main, [])

    assert result.exit_code != 0
    assert "Missing option" in result.output


def test_cli_successful_check(test_files):
    """Test CLI with successful column check."""
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--manifest",
            str(test_files["manifest"]),
            "--compiled-sql",
            str(test_files["sql_dir"]),
            "--dialect",
            "postgres",
        ],
    )

    assert result.exit_code == 0
    assert "Starting databuildcheck" in result.output
    assert "Found 1 model(s) in manifest" in result.output
    assert "All checks passed" in result.output


def test_cli_verbose_output(test_files):
    """Test CLI with verbose output."""
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--manifest",
            str(test_files["manifest"]),
            "--compiled-sql",
            str(test_files["sql_dir"]),
            "--dialect",
            "postgres",
            "--verbose",
        ],
    )

    assert result.exit_code == 0
    assert "Manifest file:" in result.output
    assert "Compiled SQL path:" in result.output
    assert "SQL dialect:" in result.output


def test_cli_nonexistent_manifest():
    """Test CLI with nonexistent manifest file."""
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--manifest",
            "nonexistent.json",
            "--compiled-sql",
            "/tmp",
            "--dialect",
            "postgres",
        ],
    )

    assert result.exit_code == 2  # Click error for invalid path
    assert "does not exist" in result.output


def test_cli_column_mismatch(test_files):
    """Test CLI with column mismatch."""
    # Modify the SQL file to have different columns
    sql_file = test_files["sql_dir"] / "models" / "users.sql"
    sql_content = """
    SELECT
        id,
        name
        -- missing email column
    FROM raw_users
    """
    sql_file.write_text(sql_content)

    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--manifest",
            str(test_files["manifest"]),
            "--compiled-sql",
            str(test_files["sql_dir"]),
            "--dialect",
            "postgres",
        ],
    )

    assert result.exit_code == 1
    assert "Column mismatch detected" in result.output
    assert "Missing in SQL: email" in result.output
    assert "Some checks failed" in result.output


def test_cli_with_table_check(test_files):
    """Test CLI with table reference checking enabled."""
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--manifest",
            str(test_files["manifest"]),
            "--compiled-sql",
            str(test_files["sql_dir"]),
            "--dialect",
            "postgres",
            "--check-tables",
        ],
    )

    if result.exit_code != 0:
        print(f"CLI output: {result.output}")
    assert result.exit_code == 0
    assert "Checking table references" in result.output


def test_cli_with_substitutions(test_files):
    """Test CLI with database and schema substitutions."""
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--manifest",
            str(test_files["manifest"]),
            "--compiled-sql",
            str(test_files["sql_dir"]),
            "--dialect",
            "postgres",
            "--check-tables",
            "--database-substitution",
            "old_db=new_db",
            "--schema-substitution",
            "old_schema=new_schema",
            "--verbose",
        ],
    )

    if result.exit_code != 0:
        print(f"CLI output: {result.output}")
    assert result.exit_code == 0
    assert "Substitutions:" in result.output
    assert "Database: old_db → new_db" in result.output
    assert "Schema: old_schema → new_schema" in result.output


def test_cli_invalid_substitution_format(test_files):
    """Test CLI with invalid substitution format."""
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--manifest",
            str(test_files["manifest"]),
            "--compiled-sql",
            str(test_files["sql_dir"]),
            "--dialect",
            "postgres",
            "--database-substitution",
            "invalid_format",
        ],
    )

    assert result.exit_code == 1
    assert "Invalid substitution format" in result.output


def test_cli_with_requirements_check(test_files):
    """Test CLI with requirements checking enabled."""
    # Create a simple config file
    config_content = {
        "required_columns": {
            "always": [{"name": "created_at", "data_type": "timestamp"}]
        },
        "model_requirements": {"require_description": False},
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(config_content, f)
        config_path = Path(f.name)

    try:
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "--manifest",
                str(test_files["manifest"]),
                "--compiled-sql",
                str(test_files["sql_dir"]),
                "--dialect",
                "postgres",
                "--check-requirements",
                "--requirements-config",
                str(config_path),
            ],
        )

        # Should fail because the test model doesn't have created_at column
        assert result.exit_code == 1
        assert "Checking manifest requirements" in result.output
    finally:
        config_path.unlink()


def test_cli_requirements_without_config(test_files):
    """Test CLI with requirements checking but no config file."""
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "--manifest",
            str(test_files["manifest"]),
            "--compiled-sql",
            str(test_files["sql_dir"]),
            "--dialect",
            "postgres",
            "--check-requirements",
        ],
    )

    assert result.exit_code == 1
    assert (
        "--requirements-config is required when --check-requirements is used"
        in result.output
    )
