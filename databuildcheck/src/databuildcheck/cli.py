# SPDX-FileCopyrightText: 2025-present Miguel Paraz <mparaz@mparaz.com>
#
# SPDX-License-Identifier: MIT

"""Command line interface for databuildcheck."""

from pathlib import Path

import click

from databuildcheck.checks.sql_column_check import SqlColumnChecker
from databuildcheck.manifest import DbtManifest


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
def main(
    manifest: Path,
    compiled_sql: Path,
    dialect: str,
    verbose: bool,
) -> None:
    """Check dbt models for consistency between manifest and compiled SQL."""
    click.echo("🔍 Starting databuildcheck...")

    if verbose:
        click.echo(f"📁 Manifest file: {manifest}")
        click.echo(f"📁 Compiled SQL path: {compiled_sql}")
        click.echo(f"🗣️  SQL dialect: {dialect}")

    try:
        # Load the manifest
        click.echo("📖 Loading dbt manifest...")
        dbt_manifest = DbtManifest(manifest)

        model_nodes = dbt_manifest.get_model_nodes()
        click.echo(f"✅ Found {len(model_nodes)} model(s) in manifest")

        # Initialize the SQL column checker
        checker = SqlColumnChecker(dbt_manifest, compiled_sql, dialect)

        # Run checks on all models
        click.echo("🔍 Checking model columns...")
        results = checker.check_all_models()

        # Process and display results
        total_models = len(results)
        successful_checks = 0
        failed_checks = 0

        for result in results:
            node_id = result["node_id"]

            if result["errors"]:
                failed_checks += 1
                click.echo(f"❌ {node_id}: {', '.join(result['errors'])}")
                continue

            if not result["columns_match"]:
                failed_checks += 1
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
            else:
                successful_checks += 1
                if verbose:
                    click.echo(f"✅ {node_id}: Columns match")

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
