# SPDX-FileCopyrightText: 2025-present Miguel Paraz <mparaz@mparaz.com>
#
# SPDX-License-Identifier: MIT

"""Module for checking SQL columns against dbt model definitions."""

from pathlib import Path
from typing import Any, Dict, List, Set

import sqlglot
from sqlglot import expressions as exp

from databuildcheck.manifest import DbtManifest


class SqlColumnChecker:
    """Class for checking SQL columns against dbt model definitions."""

    def __init__(
        self,
        manifest: DbtManifest,
        compiled_sql_path: str | Path,
        sql_dialect: str,
    ) -> None:
        """Initialize the SQL column checker.

        Args:
            manifest: Loaded dbt manifest
            compiled_sql_path: Path to compiled SQL files
            sql_dialect: SQL dialect to use for parsing (e.g., 'snowflake', 'bigquery')
        """
        self.manifest = manifest
        self.compiled_sql_path = Path(compiled_sql_path)
        self.sql_dialect = sql_dialect

    def _get_sql_file_path(self, original_file_path: str) -> Path:
        """Get the full path to the compiled SQL file.

        Args:
            original_file_path: The original_file_path from the manifest

        Returns:
            Full path to the compiled SQL file
        """
        # Remove the .sql extension and add it back to ensure consistency
        sql_file = Path(original_file_path)
        if sql_file.suffix == ".sql":
            sql_file = sql_file.with_suffix("")

        return self.compiled_sql_path / f"{sql_file}.sql"

    def _parse_sql_file(self, sql_file_path: Path) -> exp.Expression | None:
        """Parse a SQL file using sqlglot.

        Args:
            sql_file_path: Path to the SQL file

        Returns:
            Parsed SQL expression or None if parsing fails
        """
        if not sql_file_path.exists():
            return None

        try:
            with open(sql_file_path, encoding="utf-8") as f:
                sql_content = f.read()

            # Parse the SQL using sqlglot
            parsed = sqlglot.parse_one(sql_content, dialect=self.sql_dialect)
            return parsed
        except Exception:
            # Return None if parsing fails
            return None

    def _extract_columns_from_sql(self, parsed_sql: exp.Expression) -> Set[str]:
        """Extract column names from the final SELECT statement in parsed SQL.

        Args:
            parsed_sql: Parsed SQL expression

        Returns:
            Set of column names found in the final SELECT statement
        """
        columns = set()

        # Get the outermost/final SELECT statement
        # This handles cases with CTEs, subqueries, etc.
        final_select = None

        if isinstance(parsed_sql, exp.Select):
            # Direct SELECT statement
            final_select = parsed_sql
        elif hasattr(parsed_sql, "find"):
            # Find the outermost SELECT (not nested in CTEs or subqueries)
            # We want the top-level SELECT that produces the final result
            if isinstance(parsed_sql, exp.Query):
                # For queries with CTEs, get the final SELECT after the CTEs
                final_select = parsed_sql.find(exp.Select)
            else:
                # For other cases, find the first SELECT
                final_select = parsed_sql.find(exp.Select)

        if final_select:
            for expression in final_select.expressions:
                if isinstance(expression, exp.Alias):
                    # Use the alias name
                    columns.add(expression.alias)
                elif isinstance(expression, exp.Column):
                    # Use the column name
                    columns.add(expression.name)
                elif hasattr(expression, "name") and expression.name:
                    # For other expressions that have a name
                    columns.add(expression.name)

        return columns

    def check_model_columns(self, node_id: str) -> Dict[str, Any]:
        """Check columns for a specific model.

        Args:
            node_id: The unique_id of the model node

        Returns:
            Dictionary containing check results
        """
        # Get model information from manifest
        original_file_path = self.manifest.get_model_original_file_path(node_id)
        manifest_columns = self.manifest.get_model_columns(node_id)

        result = {
            "node_id": node_id,
            "original_file_path": original_file_path,
            "sql_file_exists": False,
            "sql_parsed": False,
            "manifest_columns": set(manifest_columns.keys())
            if manifest_columns
            else set(),
            "sql_columns": set(),
            "columns_match": False,
            "missing_in_sql": set(),
            "extra_in_sql": set(),
            "errors": [],
        }

        if not original_file_path:
            result["errors"].append("No original_file_path found in manifest")
            return result

        # Get SQL file path
        sql_file_path = self._get_sql_file_path(original_file_path)
        result["sql_file_path"] = str(sql_file_path)

        if not sql_file_path.exists():
            result["errors"].append(f"SQL file not found: {sql_file_path}")
            return result

        result["sql_file_exists"] = True

        # Parse SQL file
        parsed_sql = self._parse_sql_file(sql_file_path)
        if parsed_sql is None:
            result["errors"].append(f"Failed to parse SQL file: {sql_file_path}")
            return result

        result["sql_parsed"] = True

        # Extract columns from SQL
        sql_columns = self._extract_columns_from_sql(parsed_sql)
        result["sql_columns"] = sql_columns

        # Compare columns
        manifest_column_names = result["manifest_columns"]
        result["missing_in_sql"] = manifest_column_names - sql_columns
        result["extra_in_sql"] = sql_columns - manifest_column_names
        result["columns_match"] = (
            len(result["missing_in_sql"]) == 0 and len(result["extra_in_sql"]) == 0
        )

        return result

    def check_all_models(self) -> List[Dict[str, Any]]:
        """Check columns for all models in the manifest.

        Returns:
            List of check results for all models
        """
        results = []
        model_nodes = self.manifest.get_model_nodes()

        for node_id in model_nodes:
            result = self.check_model_columns(node_id)
            results.append(result)

        return results
