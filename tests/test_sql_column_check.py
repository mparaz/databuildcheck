# SPDX-FileCopyrightText: 2025-present Miguel Paraz <mparaz@mparaz.com>
#
# SPDX-License-Identifier: MIT

"""Tests for the SQL column check module."""

import json
import tempfile
from pathlib import Path

import pytest

from databuildcheck.checks.sql_column_check import SqlColumnChecker
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
                "original_file_path": "models/users.sql",
                "columns": {
                    "id": {"name": "id", "data_type": "integer"},
                    "name": {"name": "name", "data_type": "varchar"},
                    "email": {"name": "email", "data_type": "varchar"},
                },
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


@pytest.fixture
def sql_files_dir():
    """Create a temporary directory with SQL files for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        sql_dir = Path(temp_dir)

        # Create models subdirectory
        models_dir = sql_dir / "models"
        models_dir.mkdir()

        # Create a sample SQL file
        sql_file = models_dir / "users.sql"
        sql_content = """
        SELECT
            id,
            name,
            email
        FROM raw_users
        """
        sql_file.write_text(sql_content)

        yield sql_dir


@pytest.fixture
def dbt_manifest(manifest_file):
    """Create a DbtManifest instance for testing."""
    return DbtManifest(manifest_file)


def test_sql_column_checker_init(dbt_manifest, sql_files_dir):
    """Test SqlColumnChecker initialization."""
    checker = SqlColumnChecker(dbt_manifest, sql_files_dir, "postgres")

    assert checker.manifest == dbt_manifest
    assert checker.compiled_sql_path == sql_files_dir
    assert checker.sql_dialect == "postgres"


def test_get_sql_file_path(dbt_manifest, sql_files_dir):
    """Test getting SQL file path."""
    checker = SqlColumnChecker(dbt_manifest, sql_files_dir, "postgres")

    # Test with .sql extension
    path = checker._get_sql_file_path("models/users.sql")
    expected = sql_files_dir / "models" / "users.sql"
    assert path == expected

    # Test without .sql extension
    path = checker._get_sql_file_path("models/users")
    assert path == expected


def test_parse_sql_file(dbt_manifest, sql_files_dir):
    """Test parsing SQL file."""
    checker = SqlColumnChecker(dbt_manifest, sql_files_dir, "postgres")

    sql_file = sql_files_dir / "models" / "users.sql"
    parsed = checker._parse_sql_file(sql_file)

    assert parsed is not None
    assert str(parsed).strip().startswith("SELECT")


def test_parse_sql_file_nonexistent(dbt_manifest, sql_files_dir):
    """Test parsing nonexistent SQL file."""
    checker = SqlColumnChecker(dbt_manifest, sql_files_dir, "postgres")

    nonexistent_file = sql_files_dir / "nonexistent.sql"
    parsed = checker._parse_sql_file(nonexistent_file)

    assert parsed is None


def test_extract_columns_from_sql(dbt_manifest, sql_files_dir):
    """Test extracting columns from parsed SQL."""
    checker = SqlColumnChecker(dbt_manifest, sql_files_dir, "postgres")

    sql_file = sql_files_dir / "models" / "users.sql"
    parsed = checker._parse_sql_file(sql_file)
    columns = checker._extract_columns_from_sql(parsed)

    expected_columns = {"id", "name", "email"}
    assert columns == expected_columns


def test_check_model_columns_success(dbt_manifest, sql_files_dir):
    """Test checking model columns with matching columns."""
    checker = SqlColumnChecker(dbt_manifest, sql_files_dir, "postgres")

    result = checker.check_model_columns("model.my_project.users")

    assert result["node_id"] == "model.my_project.users"
    assert result["sql_file_exists"] is True
    assert result["sql_parsed"] is True
    assert result["columns_match"] is True
    assert len(result["errors"]) == 0
    assert result["manifest_columns"] == {"id", "name", "email"}
    assert result["sql_columns"] == {"id", "name", "email"}


def test_check_model_columns_missing_file(dbt_manifest, sql_files_dir):
    """Test checking model columns with missing SQL file."""
    checker = SqlColumnChecker(dbt_manifest, sql_files_dir, "postgres")

    # Create a manifest with a model that doesn't have a corresponding SQL file
    manifest_data = {
        "metadata": {"dbt_schema_version": "v12", "dbt_version": "1.8.0"},
        "nodes": {
            "model.my_project.missing": {
                "name": "missing",
                "unique_id": "model.my_project.missing",
                "resource_type": "model",
                "package_name": "my_project",
                "original_file_path": "models/missing.sql",
                "columns": {"id": {"name": "id", "data_type": "integer"}},
            },
        },
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(manifest_data, f)
        manifest_path = Path(f.name)

    try:
        manifest = DbtManifest(manifest_path)
        checker = SqlColumnChecker(manifest, sql_files_dir, "postgres")

        result = checker.check_model_columns("model.my_project.missing")

        assert result["sql_file_exists"] is False
        assert "SQL file not found" in result["errors"][0]
    finally:
        manifest_path.unlink()


def test_check_all_models(dbt_manifest, sql_files_dir):
    """Test checking all models."""
    checker = SqlColumnChecker(dbt_manifest, sql_files_dir, "postgres")

    results = checker.check_all_models()

    assert len(results) == 1
    assert results[0]["node_id"] == "model.my_project.users"
    assert results[0]["columns_match"] is True


def test_extract_columns_with_cte(dbt_manifest, sql_files_dir):
    """Test extracting columns from SQL with CTEs - should only get final SELECT columns."""
    checker = SqlColumnChecker(dbt_manifest, sql_files_dir, "postgres")

    # Create a SQL file with CTE
    sql_file = sql_files_dir / "models" / "cte_test.sql"
    sql_content = """
    WITH user_stats AS (
        SELECT
            user_id,
            count(*) as order_count,
            sum(amount) as total_amount
        FROM orders
        GROUP BY user_id
    ),
    active_users AS (
        SELECT
            user_id,
            first_name,
            last_name
        FROM users
        WHERE active = true
    )
    SELECT
        u.user_id as id,
        u.first_name as name,
        u.last_name as surname,
        s.order_count,
        s.total_amount
    FROM active_users u
    LEFT JOIN user_stats s ON u.user_id = s.user_id
    """
    sql_file.write_text(sql_content)

    parsed = checker._parse_sql_file(sql_file)
    columns = checker._extract_columns_from_sql(parsed)

    # Should only contain columns from the final SELECT, not from CTEs
    expected_columns = {"id", "name", "surname", "order_count", "total_amount"}
    assert columns == expected_columns

    # Should NOT contain columns from CTEs like user_id, first_name, last_name, etc.
    cte_columns = {"user_id", "first_name", "last_name"}
    assert not any(col in columns for col in cte_columns)
