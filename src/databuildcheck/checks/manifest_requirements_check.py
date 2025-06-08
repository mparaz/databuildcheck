# SPDX-FileCopyrightText: 2025-present Miguel Paraz <mparaz@mparaz.com>
#
# SPDX-License-Identifier: MIT

"""Module for checking dbt manifest requirements based on configuration."""

import re
from pathlib import Path
from typing import Any, Dict, List

import yaml

from databuildcheck.manifest import DbtManifest


class ManifestRequirementsChecker:
    """Class for checking dbt manifest requirements based on configuration."""

    def __init__(
        self,
        manifest: DbtManifest,
        config_path: str | Path,
    ) -> None:
        """Initialize the manifest requirements checker.

        Args:
            manifest: Loaded dbt manifest
            config_path: Path to the configuration YAML file
        """
        self.manifest = manifest
        self.config_path = Path(config_path)
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        """Load the configuration file.

        Returns:
            Configuration dictionary

        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config file is invalid YAML
        """
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        try:
            with open(self.config_path, encoding="utf-8") as f:
                config = yaml.safe_load(f)
            return config or {}
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in configuration file: {e}") from e

    def _is_model_exempt(self, model_name: str, exemption_type: str) -> bool:
        """Check if a model is exempt from certain requirements.

        Args:
            model_name: Name of the model
            exemption_type: Type of exemption to check

        Returns:
            True if model is exempt, False otherwise
        """
        exclusions = self.config.get("exclusions", {})
        exempt_patterns = exclusions.get(exemption_type, [])

        for pattern in exempt_patterns:
            if re.match(pattern.replace("*", ".*"), model_name):
                return True

        return False

    def _get_required_columns_for_model(
        self, model_data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Get all required columns for a specific model.

        Args:
            model_data: Model data from manifest

        Returns:
            List of required column specifications
        """
        required_columns = []

        # Always required columns
        always_required = self.config.get("required_columns", {}).get("always", [])
        required_columns.extend(always_required)

        # Materialization-based requirements
        materialization = model_data.get("config", {}).get("materialized", "view")
        mat_requirements = self.config.get("materialization_requirements", {})
        if materialization in mat_requirements:
            mat_columns = mat_requirements[materialization].get("required_columns", [])
            required_columns.extend(mat_columns)

        # Incremental strategy requirements (only for incremental models)
        if materialization == "incremental":
            strategy = model_data.get("config", {}).get("incremental_strategy", "merge")
            strategy_requirements = self.config.get(
                "incremental_strategy_requirements", {}
            )
            if strategy in strategy_requirements:
                strategy_columns = strategy_requirements[strategy].get(
                    "required_columns", []
                )
                required_columns.extend(strategy_columns)

        # Tag-based requirements
        tags = model_data.get("tags", [])
        tag_requirements = self.config.get("tag_requirements", {})
        for tag in tags:
            if tag in tag_requirements:
                tag_columns = tag_requirements[tag].get("required_columns", [])
                required_columns.extend(tag_columns)

        # Package-based requirements
        package_name = model_data.get("package_name", "")
        package_requirements = self.config.get("package_requirements", {})
        if package_name in package_requirements:
            package_columns = package_requirements[package_name].get(
                "required_columns", []
            )
            required_columns.extend(package_columns)

        return required_columns

    def _validate_column_requirements(
        self,
        model_name: str,
        model_columns: Dict[str, Any],
        required_columns: List[Dict[str, Any]],
    ) -> List[str]:
        """Validate that model has all required columns.

        Args:
            model_name: Name of the model
            model_columns: Columns defined in the model
            required_columns: List of required column specifications

        Returns:
            List of validation errors
        """
        errors = []
        model_column_names = set(model_columns.keys())

        for req_col in required_columns:
            col_name = req_col.get("name", "")

            # Check if column exists
            if col_name not in model_column_names:
                errors.append(f"Missing required column: {col_name}")
                continue

            # Check if column has required description
            model_col = model_columns[col_name]
            if req_col.get("description") and not model_col.get("description"):
                errors.append(f"Column '{col_name}' missing required description")

            # Check if column has required data type
            if req_col.get("data_type"):
                model_data_type = model_col.get("data_type", "").lower()
                required_data_type = req_col.get("data_type", "").lower()
                if model_data_type != required_data_type:
                    errors.append(
                        f"Column '{col_name}' has data type '{model_data_type}', "
                        f"expected '{required_data_type}'"
                    )



        return errors

    def _validate_column_descriptions(
        self, model_name: str, model_columns: Dict[str, Any]
    ) -> List[str]:
        """Validate column description requirements.

        Args:
            model_name: Name of the model
            model_columns: Columns defined in the model

        Returns:
            List of validation errors
        """
        errors = []

        if self._is_model_exempt(model_name, "description_exempt"):
            return errors

        require_descriptions = self.config.get("column_validation", {}).get(
            "require_descriptions", []
        )

        for col_name in require_descriptions:
            if col_name in model_columns:
                if not model_columns[col_name].get("description"):
                    errors.append(f"Column '{col_name}' requires a description")

        return errors

    def _validate_model_requirements(
        self, model_name: str, model_data: Dict[str, Any]
    ) -> List[str]:
        """Validate model-level requirements.

        Args:
            model_name: Name of the model
            model_data: Model data from manifest

        Returns:
            List of validation errors
        """
        errors = []
        model_requirements = self.config.get("model_requirements", {})

        # Check if model requires description
        if model_requirements.get("require_description", False):
            if not model_data.get("description"):
                errors.append("Model missing required description")

        return errors

    def check_model_requirements(self, node_id: str) -> Dict[str, Any]:
        """Check requirements for a specific model.

        Args:
            node_id: The unique_id of the model node

        Returns:
            Dictionary containing check results
        """
        model_nodes = self.manifest.get_model_nodes()

        if node_id not in model_nodes:
            return {
                "node_id": node_id,
                "requirements_valid": False,
                "errors": [f"Model not found in manifest: {node_id}"],
                "warnings": [],
            }

        model_data = model_nodes[node_id]
        model_name = model_data.get("name", "")
        model_columns = model_data.get("columns", {})

        result = {
            "node_id": node_id,
            "model_name": model_name,
            "requirements_valid": True,
            "errors": [],
            "warnings": [],
        }

        # Skip fully exempt models
        if self._is_model_exempt(model_name, "fully_exempt"):
            result["warnings"].append("Model is fully exempt from requirements")
            return result

        # Get required columns for this model
        required_columns = self._get_required_columns_for_model(model_data)

        # Validate column requirements
        column_errors = self._validate_column_requirements(
            model_name, model_columns, required_columns
        )
        result["errors"].extend(column_errors)

        # Validate column descriptions
        description_errors = self._validate_column_descriptions(
            model_name, model_columns
        )
        result["errors"].extend(description_errors)

        # Validate model-level requirements
        model_errors = self._validate_model_requirements(model_name, model_data)
        result["errors"].extend(model_errors)

        # Set overall validation status
        result["requirements_valid"] = len(result["errors"]) == 0

        return result

    def check_all_models(self) -> List[Dict[str, Any]]:
        """Check requirements for all models in the manifest.

        Returns:
            List of check results for all models
        """
        results = []
        model_nodes = self.manifest.get_model_nodes()

        for node_id in model_nodes:
            result = self.check_model_requirements(node_id)
            results.append(result)

        return results
