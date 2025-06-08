# SPDX-FileCopyrightText: 2025-present Miguel Paraz <mparaz@mparaz.com>
#
# SPDX-License-Identifier: MIT

"""Command line interface for databuildcheck."""

from pathlib import Path

import click

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
            raise ValueError(f"Invalid substitution format: '{sub_str}'. Expected 'original=substitute'")

        original, substitute = sub_str.split("=", 1)
        substitutions[original.strip()] = substitute.strip()

    return substitutions


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
def main(
    manifest: Path,
    compiled_sql: Path,
    dialect: str,
    verbose: bool,
    check_tables: bool,
    database_substitution: tuple[str, ...],
    schema_substitution: tuple[str, ...],
) -> None:
    """Check dbt models for consistency between manifest and compiled SQL."""
    click.echo("🔍 Starting databuildcheck...")

    if verbose:
        click.echo(f"📁 Manifest file: {manifest}")
        click.echo(f"📁 Compiled SQL path: {compiled_sql}")
        click.echo(f"🗣️  SQL dialect: {dialect}")
        click.echo(f"🔍 Check tables: {check_tables}")

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

        # Load the manifest
        click.echo("📖 Loading dbt manifest...")
        dbt_manifest = DbtManifest(manifest)

        model_nodes = dbt_manifest.get_model_nodes()
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
                schema_substitutions
            )

        # Run column checks
        click.echo("🔍 Checking model columns...")
        column_results = column_checker.check_all_models()

        # Run table reference checks if enabled
        table_results = []
        if check_tables:
            click.echo("🔍 Checking table references...")
            table_results = table_checker.check_all_models()

        # Process and display results
        total_models = len(column_results)
        successful_checks = 0
        failed_checks = 0

        # Create a map of table results by node_id for easy lookup
        table_results_map = {}
        if table_results:
            table_results_map = {result["node_id"]: result for result in table_results}

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

            # If no failures detected, count as successful
            if not model_failed:
                successful_checks += 1
                if verbose:
                    checks_passed = ["Columns match"]
                    if node_id in table_results_map:
                        checks_passed.append("Table references valid")
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
