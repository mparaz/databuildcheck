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
