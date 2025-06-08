#!/usr/bin/env python3
"""Test to verify the patch_path package prefix stripping works correctly."""

from databuildcheck.manifest import DbtManifest


def test_patch_path_stripping():
    """Test that patch_path package prefixes are correctly stripped."""
    # Create a mock manifest data structure with realistic patch_path values
    mock_manifest_data = {
        "nodes": {
            "model.jaffle_shop.stg_customers": {
                "name": "stg_customers",
                "original_file_path": "models/staging/stg_customers.sql",
                "patch_path": "jaffle_shop://models/staging/stg_customers.yml",
                "resource_type": "model",
            },
            "model.jaffle_shop.stg_orders": {
                "name": "stg_orders",
                "original_file_path": "models/staging/stg_orders.sql",
                "patch_path": "jaffle_shop://models/staging/stg_orders.yml",
                "resource_type": "model",
            },
            "model.jaffle_shop.customers": {
                "name": "customers",
                "original_file_path": "models/customers.sql",
                "patch_path": "jaffle_shop://models/schema.yml",
                "resource_type": "model",
            },
            "model.other_package.products": {
                "name": "products",
                "original_file_path": "models/products.sql",
                "patch_path": "other_package://models/products_schema.yml",
                "resource_type": "model",
            },
        }
    }

    # Test with restriction that should match the stripped patch_path
    restrict_files = {
        "models/staging/stg_customers.yml",  # Should match stg_customers after stripping jaffle_shop://
        "models/customers.sql",  # Should match customers by original_file_path
    }

    manifest = DbtManifest.__new__(DbtManifest)
    manifest.restrict_to_files = restrict_files
    manifest._manifest_data = mock_manifest_data

    filtered_models = manifest.get_model_nodes()

    # Should include:
    # - stg_customers (patch_path matches after stripping prefix)
    # - customers (original_file_path matches)
    # Should NOT include:
    # - stg_orders (patch_path doesn't match after stripping)
    # - products (neither path matches)

    print(f"Filtered models: {list(filtered_models.keys())}")

    assert len(filtered_models) == 2, f"Expected 2 models, got {len(filtered_models)}"
    assert "model.jaffle_shop.stg_customers" in filtered_models, (
        "stg_customers should be included (patch_path match)"
    )
    assert "model.jaffle_shop.customers" in filtered_models, (
        "customers should be included (original_file_path match)"
    )
    assert "model.jaffle_shop.stg_orders" not in filtered_models, (
        "stg_orders should be excluded"
    )
    assert "model.other_package.products" not in filtered_models, (
        "products should be excluded"
    )

    print("✅ Patch path stripping test passed")

    # Test edge case: patch_path without package prefix
    mock_manifest_data["nodes"]["model.test.no_prefix"] = {
        "name": "no_prefix",
        "original_file_path": "models/no_prefix.sql",
        "patch_path": "models/no_prefix.yml",  # No package prefix
        "resource_type": "model",
    }

    restrict_files_2 = {"models/no_prefix.yml"}
    manifest.restrict_to_files = restrict_files_2

    filtered_models_2 = manifest.get_model_nodes()
    assert "model.test.no_prefix" in filtered_models_2, (
        "Model with patch_path without prefix should be included"
    )

    print("✅ Patch path without prefix test passed")


if __name__ == "__main__":
    test_patch_path_stripping()
    print("🎉 All patch_path tests passed!")
