# SPDX-FileCopyrightText: 2025-present Miguel Paraz <mparaz@mparaz.com>
#
# SPDX-License-Identifier: MIT

"""Module for loading and working with dbt manifest files."""

import json
from pathlib import Path
from typing import Any, Dict, List


class DbtManifest:
    """Class for loading and working with dbt manifest.json files."""

    def __init__(self, manifest_path: str | Path) -> None:
        """Initialize the manifest loader.

        Args:
            manifest_path: Path to the manifest.json file
        """
        self.manifest_path = Path(manifest_path)
        self._manifest_data: Dict[str, Any] = {}
        self._load_manifest()

    def _load_manifest(self) -> None:
        """Load the manifest.json file."""
        if not self.manifest_path.exists():
            raise FileNotFoundError(f"Manifest file not found: {self.manifest_path}")

        if not self.manifest_path.is_file():
            raise ValueError(f"Manifest path is not a file: {self.manifest_path}")

        try:
            with open(self.manifest_path, encoding="utf-8") as f:
                self._manifest_data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in manifest file: {e}") from e

    @property
    def nodes(self) -> Dict[str, Any]:
        """Get all nodes from the manifest."""
        return self._manifest_data.get("nodes", {})

    @property
    def sources(self) -> Dict[str, Any]:
        """Get all sources from the manifest."""
        return self._manifest_data.get("sources", {})

    def get_model_nodes(self) -> Dict[str, Any]:
        """Get all model nodes from the manifest.

        Returns:
            Dictionary of model nodes (nodes with unique_id starting with 'model.')
        """
        return {
            node_id: node_data
            for node_id, node_data in self.nodes.items()
            if node_id.startswith("model.")
        }

    def get_model_columns(self, node_id: str) -> Dict[str, Any]:
        """Get columns for a specific model node.

        Args:
            node_id: The unique_id of the model node

        Returns:
            Dictionary of columns for the model
        """
        node = self.nodes.get(node_id, {})
        return node.get("columns", {})

    def get_model_original_file_path(self, node_id: str) -> str | None:
        """Get the original_file_path for a specific model node.

        Args:
            node_id: The unique_id of the model node

        Returns:
            The original_file_path if it exists, None otherwise
        """
        node = self.nodes.get(node_id, {})
        return node.get("original_file_path")

    def get_models_info(self) -> List[Dict[str, Any]]:
        """Get information about all models including their paths and columns.

        Returns:
            List of dictionaries containing model information
        """
        models_info = []

        for node_id, node_data in self.get_model_nodes().items():
            model_info = {
                "node_id": node_id,
                "name": node_data.get("name"),
                "original_file_path": node_data.get("original_file_path"),
                "columns": node_data.get("columns", {}),
                "resource_type": node_data.get("resource_type"),
                "package_name": node_data.get("package_name"),
            }
            models_info.append(model_info)

        return models_info

    def get_source_nodes(self) -> Dict[str, Any]:
        """Get all source nodes from the manifest.

        Returns:
            Dictionary of source nodes (nodes with unique_id starting with 'source.')
        """
        return {
            source_id: source_data
            for source_id, source_data in self.sources.items()
            if source_id.startswith("source.")
        }

    def get_all_referenceable_tables(self) -> Dict[str, Dict[str, str]]:
        """Get all tables that can be referenced in SQL (models + sources).

        Returns:
            Dictionary mapping table identifiers to their metadata.
            Keys are in format: database.schema.table or schema.table or table
        """
        referenceable_tables = {}

        # Add models
        for node_id, node_data in self.get_model_nodes().items():
            name = node_data.get("name", "")
            database = node_data.get("database", "")
            schema = node_data.get("schema", "")

            # Create various possible reference formats
            table_refs = self._generate_table_references(name, schema, database)
            for ref in table_refs:
                referenceable_tables[ref] = {
                    "type": "model",
                    "unique_id": node_id,
                    "name": name,
                    "database": database,
                    "schema": schema,
                }

        # Add sources
        for source_id, source_data in self.get_source_nodes().items():
            name = source_data.get("name", "")
            database = source_data.get("database", "")
            schema = source_data.get("schema", "")

            # Create various possible reference formats
            table_refs = self._generate_table_references(name, schema, database)
            for ref in table_refs:
                referenceable_tables[ref] = {
                    "type": "source",
                    "unique_id": source_id,
                    "name": name,
                    "database": database,
                    "schema": schema,
                }

        return referenceable_tables

    def _generate_table_references(
        self, name: str, schema: str, database: str
    ) -> List[str]:
        """Generate possible table reference formats for a given table.

        Args:
            name: Table name
            schema: Schema name
            database: Database name

        Returns:
            List of possible reference formats
        """
        refs = []

        # Add the table name alone
        if name:
            refs.append(name.lower())

        # Add schema.table format
        if schema and name:
            refs.append(f"{schema.lower()}.{name.lower()}")

        # Add database.schema.table format
        if database and schema and name:
            refs.append(f"{database.lower()}.{schema.lower()}.{name.lower()}")

        return refs
