# SPDX-FileCopyrightText: 2025-present Miguel Paraz <mparaz@mparaz.com>
#
# SPDX-License-Identifier: MIT

"""Module for checking SQL table references against dbt manifest."""

from pathlib import Path
from typing import Any, Dict, List, Set

import sqlglot
from sqlglot import expressions as exp

from databuildcheck.manifest import DbtManifest


class SqlTableChecker:
    """Class for checking SQL table references against dbt manifest."""

    def __init__(
        self,
        manifest: DbtManifest,
        compiled_sql_path: str | Path,
        sql_dialect: str,
        database_substitutions: Dict[str, str] | None = None,
        schema_substitutions: Dict[str, str] | None = None,
    ) -> None:
        """Initialize the SQL table checker.

        Args:
            manifest: Loaded dbt manifest
            compiled_sql_path: Path to compiled SQL files
            sql_dialect: SQL dialect to use for parsing
            database_substitutions: Dict mapping original database names to substitutes
            schema_substitutions: Dict mapping original schema names to substitutes
        """
        self.manifest = manifest
        self.compiled_sql_path = Path(compiled_sql_path)
        self.sql_dialect = sql_dialect
        self.database_substitutions = database_substitutions or {}
        self.schema_substitutions = schema_substitutions or {}

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

    def _extract_table_references(self, parsed_sql: exp.Expression) -> Set[str]:
        """Extract all table references from parsed SQL, excluding CTEs.

        Args:
            parsed_sql: Parsed SQL expression

        Returns:
            Set of table references found in the SQL (excluding CTE names)
        """
        table_refs = set()
        cte_names = set()

        # First, collect all CTE names to exclude them
        for cte in parsed_sql.find_all(exp.CTE):
            if cte.alias:
                cte_names.add(cte.alias.lower())

        # Find all table references in FROM and JOIN clauses
        for table in parsed_sql.find_all(exp.Table):
            table_name = self._normalize_table_reference(table)
            if table_name:
                # Check if this is a CTE reference (single name that matches a CTE)
                if "." not in table_name and table_name.lower() in cte_names:
                    # Skip CTE references
                    continue
                table_refs.add(table_name)

        return table_refs

    def _normalize_table_reference(self, table: exp.Table) -> str | None:
        """Normalize a table reference and apply substitutions.

        Args:
            table: SQLGlot Table expression

        Returns:
            Normalized table reference string or None
        """
        # Extract parts of the table reference
        parts = []

        # Get catalog (database), db (schema), and name (table)
        if table.catalog:
            parts.append(str(table.catalog))
        if table.db:
            parts.append(str(table.db))
        if table.name:
            parts.append(str(table.name))

        if not parts:
            return None

        # Apply substitutions
        if len(parts) >= 3:  # database.schema.table
            database, schema, table_name = parts[0], parts[1], parts[2]
            database = self.database_substitutions.get(database, database)
            schema = self.schema_substitutions.get(schema, schema)
            return f"{database.lower()}.{schema.lower()}.{table_name.lower()}"
        elif len(parts) == 2:  # schema.table
            schema, table_name = parts[0], parts[1]
            schema = self.schema_substitutions.get(schema, schema)
            return f"{schema.lower()}.{table_name.lower()}"
        else:  # table only
            table_name = parts[0]
            return table_name.lower()

    def _apply_table_reference_substitutions(
        self, table_references: Set[str]
    ) -> Set[str]:
        """Apply database and schema substitutions to table references after parsing.

        Args:
            table_references: Set of table references extracted from SQL

        Returns:
            Set of table references with substitutions applied
        """
        substituted_refs = set()

        for ref in table_references:
            substituted_ref = self._apply_substitutions_to_reference(ref)
            substituted_refs.add(substituted_ref)

        return substituted_refs

    def _apply_substitutions_to_reference(self, table_ref: str) -> str:
        """Apply substitutions to a single table reference.

        Args:
            table_ref: Table reference (e.g., 'database.schema.table' or 'schema.table' or 'table')

        Returns:
            Table reference with substitutions applied
        """
        parts = table_ref.split(".")

        if len(parts) == 3:  # database.schema.table
            database, schema, table = parts
            database = self.database_substitutions.get(database, database)
            schema = self.schema_substitutions.get(schema, schema)
            return f"{database}.{schema}.{table}"
        elif len(parts) == 2:  # schema.table
            schema, table = parts
            schema = self.schema_substitutions.get(schema, schema)
            return f"{schema}.{table}"
        else:  # table only
            return table_ref

    def check_model_table_references(self, node_id: str) -> Dict[str, Any]:
        """Check table references for a specific model.

        Args:
            node_id: The unique_id of the model node

        Returns:
            Dictionary containing check results
        """
        # Get model information from manifest
        original_file_path = self.manifest.get_model_original_file_path(node_id)

        result = {
            "node_id": node_id,
            "original_file_path": original_file_path,
            "sql_file_exists": False,
            "sql_parsed": False,
            "table_references": set(),
            "valid_references": set(),
            "invalid_references": set(),
            "references_valid": False,
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
        try:
            with open(sql_file_path, encoding="utf-8") as f:
                sql_content = f.read()

            parsed_sql = sqlglot.parse_one(sql_content, dialect=self.sql_dialect)

            if parsed_sql is None:
                result["errors"].append(f"Failed to parse SQL file: {sql_file_path}")
                return result

        except Exception as e:
            result["errors"].append(f"Error processing SQL file: {e}")
            return result

        result["sql_parsed"] = True

        # Extract table references

        raw_table_references = self._extract_table_references(parsed_sql)

        # Apply substitutions to table references
        table_references = self._apply_table_reference_substitutions(
            raw_table_references
        )
        result["table_references"] = table_references

        # Get all referenceable tables from manifest
        referenceable_tables = self.manifest.get_all_referenceable_tables()

        # Check which references are valid
        valid_refs = set()
        invalid_refs = set()

        for ref in table_references:
            if ref in referenceable_tables:
                valid_refs.add(ref)
            else:
                invalid_refs.add(ref)

        result["valid_references"] = valid_refs
        result["invalid_references"] = invalid_refs
        result["references_valid"] = len(invalid_refs) == 0

        return result

    def check_all_models(self) -> List[Dict[str, Any]]:
        """Check table references for all models in the manifest.

        Returns:
            List of check results for all models
        """
        results = []
        model_nodes = self.manifest.get_model_nodes()

        for node_id in model_nodes:
            result = self.check_model_table_references(node_id)
            results.append(result)

        return results
