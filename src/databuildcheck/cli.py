# SPDX-FileCopyrightText: 2025-present Miguel Paraz <mparaz@mparaz.com>
#
# SPDX-License-Identifier: MIT

"""Command line interface for databuildcheck."""

from pathlib import Path

import click

from databuildcheck.checks.manifest_requirements_check import (
    ManifestRequirementsChecker,
)
from databuildcheck.checks.sql_column_check import SqlColumnChecker
from databuildcheck.checks.sql_table_check import SqlTableChecker
from databuildcheck.manifest import DbtManifest


def _parse_substitutions(substitution_strings: tuple[str, ...]) -> dict[str, str]:
    """Parse substitution strings in format 'original=substitute'.

    Args:
        substitution_strings: Tuple of substitution strings

    Returns:
        Dictionary mapping original names to substitutes

    Raises:
        ValueError: If substitution format is invalid
    """
    substitutions = {}
    for sub_str in substitution_strings:
        if "=" not in sub_str:
            raise ValueError(
                f"Invalid substitution format: '{sub_str}'. Expected 'original=substitute'"
            )

        original, substitute = sub_str.split("=", 1)
        substitutions[original.strip()] = substitute.strip()

    return substitutions


def _load_file_list(file_path: Path) -> set[str]:
    """Load list of files from a text file.

    Args:
        file_path: Path to file containing list of files (one per line)

    Returns:
        Set of file paths (relative paths, normalized)

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file cannot be read
    """
    try:
        with open(file_path, encoding="utf-8") as f:
            files = set()
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):  # Skip empty lines and comments
                    # Normalize path separators
                    normalized_path = line.replace("\\", "/")
                    files.add(normalized_path)
            return files
    except FileNotFoundError as e:
        raise FileNotFoundError(f"File list not found: {file_path}") from e
    except Exception as e:
        raise ValueError(f"Error reading file list: {e}") from e


@click.command()
@click.option(
    "--manifest",
    "-m",
    required=True,
    type=click.Path(exists=True, path_type=Path),
    help="Path to the dbt manifest.json file",
)
@click.option(
    "--compiled-sql",
    "-c",
    required=True,
    type=click.Path(exists=True, path_type=Path),
    help="Path to the directory containing compiled SQL files",
)
@click.option(
    "--dialect",
    "-d",
    required=True,
    type=str,
    help="SQL dialect to use for parsing (e.g., snowflake, bigquery, postgres)",
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output",
)
@click.option(
    "--check-tables",
    "-t",
    is_flag=True,
    help="Enable table reference checking",
)
@click.option(
    "--database-substitution",
    multiple=True,
    help="Database name substitution in format 'original=substitute' (can be used multiple times)",
)
@click.option(
    "--schema-substitution",
    multiple=True,
    help="Schema name substitution in format 'original=substitute' (can be used multiple times)",
)
@click.option(
    "--check-requirements",
    "-r",
    is_flag=True,
    help="Enable manifest requirements checking",
)
@click.option(
    "--requirements-config",
    type=click.Path(exists=True, path_type=Path),
    help="Path to requirements configuration YAML file (required when --check-requirements is used)",
)
@click.option(
    "--restrict-to-files",
    type=click.Path(exists=True, path_type=Path),
    help="Path to file containing list of YAML and SQL files to restrict processing to (one file per line)",
)
def main(
    manifest: Path,
    compiled_sql: Path,
    dialect: str,
    verbose: bool,
    check_tables: bool,
    database_substitution: tuple[str, ...],
    schema_substitution: tuple[str, ...],
    check_requirements: bool,
    requirements_config: Path | None,
    restrict_to_files: Path | None,
) -> None:
    """Check dbt models for consistency between manifest and compiled SQL."""
    click.echo("🔍 Starting databuildcheck...")

    # Validate requirements config
    if check_requirements and not requirements_config:
        click.echo(
            "❌ Error: --requirements-config is required when --check-requirements is used"
        )
        exit(1)

    if verbose:
        click.echo(f"📁 Manifest file: {manifest}")
        click.echo(f"📁 Compiled SQL path: {compiled_sql}")
        click.echo(f"🗣️  SQL dialect: {dialect}")
        click.echo(f"🔍 Check tables: {check_tables}")
        click.echo(f"🔍 Check requirements: {check_requirements}")
        if requirements_config:
            click.echo(f"📋 Requirements config: {requirements_config}")
        if restrict_to_files:
            click.echo(f"📄 Restrict to files: {restrict_to_files}")

    try:
        # Parse substitutions
        db_substitutions = _parse_substitutions(database_substitution)
        schema_substitutions = _parse_substitutions(schema_substitution)

        if verbose and (db_substitutions or schema_substitutions):
            click.echo("🔄 Substitutions:")
            for orig, sub in db_substitutions.items():
                click.echo(f"   Database: {orig} → {sub}")
            for orig, sub in schema_substitutions.items():
                click.echo(f"   Schema: {orig} → {sub}")

        # Load file restriction list if provided
        restrict_files = None
        if restrict_to_files:
            click.echo(f"📄 Loading file restriction list from {restrict_to_files}...")
            restrict_files = _load_file_list(restrict_to_files)
            if verbose:
                click.echo(f"   Restricting to {len(restrict_files)} file(s)")

        # Load the manifest
        click.echo("📖 Loading dbt manifest...")
        dbt_manifest = DbtManifest(manifest, restrict_files)

        model_nodes = dbt_manifest.get_model_nodes()
        if restrict_to_files:
            click.echo(
                f"✅ Found {len(model_nodes)} model(s) in manifest (restricted to specified files)"
            )
        else:
            click.echo(f"✅ Found {len(model_nodes)} model(s) in manifest")

        # Initialize checkers
        column_checker = SqlColumnChecker(dbt_manifest, compiled_sql, dialect)

        table_checker = None
        if check_tables:
            table_checker = SqlTableChecker(
                dbt_manifest,
                compiled_sql,
                dialect,
                db_substitutions,
                schema_substitutions,
            )

        requirements_checker = None
        if check_requirements:
            requirements_checker = ManifestRequirementsChecker(
                dbt_manifest, requirements_config
            )

        # Run column checks
        click.echo("🔍 Checking model columns...")
        column_results = column_checker.check_all_models()

        # Run table reference checks if enabled
        table_results = []
        if check_tables:
            click.echo("🔍 Checking table references...")
            table_results = table_checker.check_all_models()

        # Run requirements checks if enabled
        requirements_results = []
        if check_requirements:
            click.echo("🔍 Checking manifest requirements...")
            requirements_results = requirements_checker.check_all_models()

        # Process and display results
        total_models = len(column_results)
        successful_checks = 0
        failed_checks = 0

        # Create maps of results by node_id for easy lookup
        table_results_map = {}
        if table_results:
            table_results_map = {result["node_id"]: result for result in table_results}

        requirements_results_map = {}
        if requirements_results:
            requirements_results_map = {
                result["node_id"]: result for result in requirements_results
            }

        for result in column_results:
            node_id = result["node_id"]
            model_failed = False

            # Check column results
            if result["errors"]:
                failed_checks += 1
                model_failed = True
                click.echo(f"❌ {node_id}: {', '.join(result['errors'])}")
            elif not result["columns_match"]:
                failed_checks += 1
                model_failed = True
                click.echo(f"⚠️  {node_id}: Column mismatch detected")

                if result["missing_in_sql"]:
                    click.echo(
                        f"   Missing in SQL: {', '.join(sorted(result['missing_in_sql']))}"
                    )

                if result["extra_in_sql"]:
                    click.echo(
                        f"   Extra in SQL: {', '.join(sorted(result['extra_in_sql']))}"
                    )

                if verbose:
                    click.echo(
                        f"   Manifest columns: {', '.join(sorted(result['manifest_columns']))}"
                    )
                    click.echo(
                        f"   SQL columns: {', '.join(sorted(result['sql_columns']))}"
                    )

            # Check table reference results if available
            if node_id in table_results_map:
                table_result = table_results_map[node_id]

                if table_result["errors"]:
                    if not model_failed:
                        failed_checks += 1
                        model_failed = True
                    click.echo(f"❌ {node_id}: {', '.join(table_result['errors'])}")
                elif not table_result["references_valid"]:
                    if not model_failed:
                        failed_checks += 1
                        model_failed = True
                    click.echo(f"⚠️  {node_id}: Invalid table references detected")

                    if table_result["invalid_references"]:
                        click.echo(
                            f"   Invalid references: {', '.join(sorted(table_result['invalid_references']))}"
                        )

                    if verbose and table_result["valid_references"]:
                        click.echo(
                            f"   Valid references: {', '.join(sorted(table_result['valid_references']))}"
                        )

            # Check requirements results if available
            if node_id in requirements_results_map:
                requirements_result = requirements_results_map[node_id]

                if requirements_result["errors"]:
                    if not model_failed:
                        failed_checks += 1
                        model_failed = True
                    click.echo(f"❌ {node_id}: Requirements validation failed")
                    for error in requirements_result["errors"]:
                        click.echo(f"   {error}")
                elif not requirements_result["requirements_valid"]:
                    if not model_failed:
                        failed_checks += 1
                        model_failed = True
                    click.echo(f"⚠️  {node_id}: Requirements not met")

                # Show warnings if any
                if verbose and requirements_result.get("warnings"):
                    for warning in requirements_result["warnings"]:
                        click.echo(f"   Warning: {warning}")

            # If no failures detected, count as successful
            if not model_failed:
                successful_checks += 1
                if verbose:
                    checks_passed = ["Columns match"]
                    if node_id in table_results_map:
                        checks_passed.append("Table references valid")
                    if node_id in requirements_results_map:
                        checks_passed.append("Requirements met")
                    click.echo(f"✅ {node_id}: {', '.join(checks_passed)}")

        # Summary
        click.echo("\n📊 Summary:")
        click.echo(f"   Total models: {total_models}")
        click.echo(f"   ✅ Passed: {successful_checks}")
        click.echo(f"   ❌ Failed: {failed_checks}")

        if failed_checks > 0:
            click.echo("\n❌ Some checks failed. Please review the output above.")
            exit(1)
        else:
            click.echo("\n🎉 All checks passed!")

    except Exception as e:
        click.echo(f"❌ Error: {e}")
        exit(1)


if __name__ == "__main__":
    main()
