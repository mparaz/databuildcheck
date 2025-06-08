# SPDX-FileCopyrightText: 2025-present Miguel Paraz <mparaz@mparaz.com>
#
# SPDX-License-Identifier: MIT

"""Tests for the SQL table check module."""

import json
import tempfile
from pathlib import Path

import pytest

from databuildcheck.checks.sql_table_check import SqlTableChecker
from databuildcheck.manifest import DbtManifest


@pytest.fixture
def sample_manifest_data():
    """Sample manifest data with models and sources for testing."""
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
                "database": "analytics",
                "schema": "public",
            },
            "model.my_project.orders": {
                "name": "orders",
                "unique_id": "model.my_project.orders",
                "resource_type": "model",
                "package_name": "my_project",
                "original_file_path": "models/orders.sql",
                "database": "analytics",
                "schema": "public",
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
            "source.my_project.raw.raw_orders": {
                "name": "raw_orders",
                "unique_id": "source.my_project.raw.raw_orders",
                "resource_type": "source",
                "package_name": "my_project",
                "database": "raw_db",
                "schema": "raw",
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

        # Create a SQL file that references valid tables
        users_sql = models_dir / "users.sql"
        users_content = """
        SELECT
            id,
            name,
            email
        FROM raw_db.raw.raw_users
        """
        users_sql.write_text(users_content)

        # Create a SQL file that references invalid tables
        orders_sql = models_dir / "orders.sql"
        orders_content = """
        SELECT
            o.id,
            o.user_id,
            u.name
        FROM raw_db.raw.raw_orders o
        LEFT JOIN analytics.public.users u ON o.user_id = u.id
        LEFT JOIN invalid_db.invalid_schema.invalid_table i ON o.id = i.order_id
        """
        orders_sql.write_text(orders_content)

        yield sql_dir


@pytest.fixture
def dbt_manifest(manifest_file):
    """Create a DbtManifest instance for testing."""
    return DbtManifest(manifest_file)


def test_sql_table_checker_init(dbt_manifest, sql_files_dir):
    """Test SqlTableChecker initialization."""
    checker = SqlTableChecker(dbt_manifest, sql_files_dir, "postgres")

    assert checker.manifest == dbt_manifest
    assert checker.compiled_sql_path == sql_files_dir
    assert checker.sql_dialect == "postgres"
    assert checker.database_substitutions == {}
    assert checker.schema_substitutions == {}


def test_sql_table_checker_with_substitutions(dbt_manifest, sql_files_dir):
    """Test SqlTableChecker initialization with substitutions."""
    db_subs = {"old_db": "new_db"}
    schema_subs = {"old_schema": "new_schema"}

    checker = SqlTableChecker(
        dbt_manifest, sql_files_dir, "postgres", db_subs, schema_subs
    )

    assert checker.database_substitutions == db_subs
    assert checker.schema_substitutions == schema_subs


def test_extract_table_references(dbt_manifest, sql_files_dir):
    """Test extracting table references from SQL."""
    checker = SqlTableChecker(dbt_manifest, sql_files_dir, "postgres")

    # Test with users.sql (simple case)
    users_sql = sql_files_dir / "models" / "users.sql"
    parsed = checker._parse_sql_file(users_sql)
    table_refs = checker._extract_table_references(parsed)

    expected_refs = {"raw_db.raw.raw_users"}
    assert table_refs == expected_refs


def test_extract_table_references_complex(dbt_manifest, sql_files_dir):
    """Test extracting table references from complex SQL with JOINs."""
    checker = SqlTableChecker(dbt_manifest, sql_files_dir, "postgres")

    # Test with orders.sql (complex case with JOINs)
    orders_sql = sql_files_dir / "models" / "orders.sql"
    parsed = checker._parse_sql_file(orders_sql)
    table_refs = checker._extract_table_references(parsed)

    expected_refs = {
        "raw_db.raw.raw_orders",
        "analytics.public.users",
        "invalid_db.invalid_schema.invalid_table",
    }
    assert table_refs == expected_refs


def test_check_model_table_references_valid(dbt_manifest, sql_files_dir):
    """Test checking model with valid table references."""
    checker = SqlTableChecker(dbt_manifest, sql_files_dir, "postgres")

    result = checker.check_model_table_references("model.my_project.users")

    assert result["node_id"] == "model.my_project.users"
    assert result["sql_file_exists"] is True
    assert result["sql_parsed"] is True
    assert result["references_valid"] is True
    assert len(result["errors"]) == 0
    assert result["table_references"] == {"raw_db.raw.raw_users"}
    assert result["valid_references"] == {"raw_db.raw.raw_users"}
    assert result["invalid_references"] == set()


def test_check_model_table_references_invalid(dbt_manifest, sql_files_dir):
    """Test checking model with invalid table references."""
    checker = SqlTableChecker(dbt_manifest, sql_files_dir, "postgres")

    result = checker.check_model_table_references("model.my_project.orders")

    assert result["node_id"] == "model.my_project.orders"
    assert result["sql_file_exists"] is True
    assert result["sql_parsed"] is True
    assert result["references_valid"] is False
    assert len(result["errors"]) == 0

    # Should have one invalid reference
    assert "invalid_db.invalid_schema.invalid_table" in result["invalid_references"]

    # Should have valid references too
    expected_valid = {"raw_db.raw.raw_orders", "analytics.public.users"}
    assert result["valid_references"] == expected_valid


def test_apply_table_reference_substitutions(dbt_manifest, sql_files_dir):
    """Test applying substitutions to table references."""
    db_subs = {"raw_db": "production_db"}
    schema_subs = {"raw": "prod_raw"}

    checker = SqlTableChecker(
        dbt_manifest, sql_files_dir, "postgres", db_subs, schema_subs
    )

    original_refs = {"raw_db.raw.users", "raw_db.raw.orders", "other_table"}
    substituted_refs = checker._apply_table_reference_substitutions(original_refs)

    expected_refs = {"production_db.prod_raw.users", "production_db.prod_raw.orders", "other_table"}
    assert substituted_refs == expected_refs


def test_apply_substitutions_to_reference(dbt_manifest, sql_files_dir):
    """Test applying substitutions to individual table references."""
    db_subs = {"raw_db": "production_db"}
    schema_subs = {"raw": "prod_raw"}

    checker = SqlTableChecker(
        dbt_manifest, sql_files_dir, "postgres", db_subs, schema_subs
    )

    # Test database.schema.table format
    result = checker._apply_substitutions_to_reference("raw_db.raw.users")
    assert result == "production_db.prod_raw.users"

    # Test schema.table format
    result = checker._apply_substitutions_to_reference("raw.users")
    assert result == "prod_raw.users"

    # Test table only format
    result = checker._apply_substitutions_to_reference("users")
    assert result == "users"

    # Test no substitution needed
    result = checker._apply_substitutions_to_reference("other_db.other_schema.users")
    assert result == "other_db.other_schema.users"


def test_check_all_models(dbt_manifest, sql_files_dir):
    """Test checking all models."""
    checker = SqlTableChecker(dbt_manifest, sql_files_dir, "postgres")

    results = checker.check_all_models()

    assert len(results) == 2

    # Find results by node_id
    users_result = next(r for r in results if r["node_id"] == "model.my_project.users")
    orders_result = next(r for r in results if r["node_id"] == "model.my_project.orders")

    # Users should be valid
    assert users_result["references_valid"] is True

    # Orders should be invalid due to the invalid table reference
    assert orders_result["references_valid"] is False


def test_extract_table_references_excludes_ctes(dbt_manifest, sql_files_dir):
    """Test that CTE names are excluded from table references."""
    checker = SqlTableChecker(dbt_manifest, sql_files_dir, "postgres")

    # Create a SQL file with CTEs
    sql_file = sql_files_dir / "models" / "cte_test.sql"
    sql_content = """
    WITH user_stats AS (
        SELECT
            user_id,
            count(*) as order_count
        FROM raw_db.raw.raw_orders
        GROUP BY user_id
    ),
    active_users AS (
        SELECT
            user_id,
            first_name
        FROM raw_db.raw.raw_users
        WHERE active = true
    )
    SELECT
        u.user_id,
        u.first_name,
        s.order_count
    FROM active_users u
    LEFT JOIN user_stats s ON u.user_id = s.user_id
    """
    sql_file.write_text(sql_content)

    parsed = checker._parse_sql_file(sql_file)
    table_refs = checker._extract_table_references(parsed)

    # Should only contain actual table references, not CTE names
    expected_refs = {"raw_db.raw.raw_orders", "raw_db.raw.raw_users"}
    assert table_refs == expected_refs

    # Should NOT contain CTE names
    cte_names = {"user_stats", "active_users"}
    assert not any(cte in table_refs for cte in cte_names)
