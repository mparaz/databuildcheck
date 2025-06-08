# databuildcheck

[![PyPI - Version](https://img.shields.io/pypi/v/databuildcheck.svg)](https://pypi.org/project/databuildcheck)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/databuildcheck.svg)](https://pypi.org/project/databuildcheck)

-----

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Development](#development)
- [License](#license)

## Installation

```console
pip install databuildcheck
```

## Usage

`databuildcheck` is a command-line tool that validates dbt models by comparing the columns defined in your dbt manifest with the actual columns in your compiled SQL files.

### Basic Usage

```console
databuildcheck --manifest path/to/manifest.json --compiled-sql path/to/compiled/sql --dialect postgres
```

### Parameters

- `--manifest` / `-m`: Path to your dbt `manifest.json` file (required)
- `--compiled-sql` / `-c`: Path to the directory containing compiled SQL files (required)
- `--dialect` / `-d`: SQL dialect for parsing (e.g., `postgres`, `snowflake`, `bigquery`) (required)
- `--verbose` / `-v`: Enable verbose output (optional)
- `--check-tables` / `-t`: Enable table reference checking (optional)
- `--database-substitution`: Database name substitution in format 'original=substitute' (optional, can be used multiple times)
- `--schema-substitution`: Schema name substitution in format 'original=substitute' (optional, can be used multiple times)
- `--check-requirements` / `-r`: Enable manifest requirements checking (optional)
- `--requirements-config`: Path to requirements configuration YAML file (required when `--check-requirements` is used)

### Example

```console
# Check columns only using PostgreSQL dialect
databuildcheck -m target/manifest.json -c target/compiled/my_project/models -d postgres

# Check both columns and table references
databuildcheck -m target/manifest.json -c target/compiled/my_project/models -d postgres --check-tables

# With database and schema substitutions (useful for different environments)
databuildcheck -m target/manifest.json -c target/compiled/my_project/models -d snowflake \
  --check-tables \
  --database-substitution "dev_db=prod_db" \
  --schema-substitution "staging=raw" \
  --verbose

# With manifest requirements checking
databuildcheck -m target/manifest.json -c target/compiled/my_project/models -d postgres \
  --check-requirements \
  --requirements-config requirements.yaml

# All checks enabled
databuildcheck -m target/manifest.json -c target/compiled/my_project/models -d postgres \
  --check-tables \
  --check-requirements \
  --requirements-config requirements.yaml \
  --verbose
```

### What it checks

The tool performs the following validations:

#### Column Checking (always enabled)
1. **Model Discovery**: Finds all models in the dbt manifest (nodes starting with "model.")
2. **SQL File Resolution**: Locates the corresponding compiled SQL file for each model
3. **SQL Parsing**: Parses the SQL using sqlglot with the specified dialect
4. **Column Extraction**: Extracts column names from the final SELECT statement (ignores CTEs and subqueries)
5. **Column Comparison**: Compares manifest columns with SQL columns and reports:
   - Missing columns (defined in manifest but not in SQL)
   - Extra columns (present in SQL but not defined in manifest)

#### Table Reference Checking (enabled with `--check-tables`)
1. **Table Reference Extraction**: Extracts all table references from FROM and JOIN clauses (excludes CTE names)
2. **Manifest Validation**: Verifies that all referenced tables exist in the dbt manifest as either:
   - Models (other dbt models)
   - Sources (external data sources defined in dbt)
3. **Substitution Support**: Applies database and schema name substitutions to extracted table references
4. **Reference Validation**: Reports any tables that are referenced but not defined in the manifest

#### Manifest Requirements Checking (enabled with `--check-requirements`)
1. **Configuration-Based Validation**: Uses YAML configuration file to define requirements
2. **Column Requirements**: Validates required columns based on:
   - Global requirements (always required)
   - Materialization type (table, view, incremental, snapshot)
   - Incremental strategy (merge, append, delete_insert)
   - Model tags (pii, financial, etc.)
   - Package name
3. **Column Validation**: Ensures columns have required descriptions and data types
4. **Model Requirements**: Validates model-level requirements (descriptions, tests, etc.)
5. **Flexible Exemptions**: Supports regex patterns for exempting models from specific requirements

#### Key Features
- **CTE Handling**: Correctly handles complex SQL with CTEs by excluding CTE names from table reference validation
- **Environment Flexibility**: Database and schema substitutions allow the same manifest to work across different environments
- **Configuration-Driven**: Requirements checking uses flexible YAML configuration for different validation rules
- **Comprehensive Coverage**: Checks column consistency, table reference validity, and manifest requirements

### Requirements Configuration

The requirements checking feature uses a YAML configuration file to define validation rules. See `sample_config.yaml` for a comprehensive example that includes:

- **Global column requirements** (always required for all models)
- **Materialization-based requirements** (table, view, incremental, snapshot)
- **Incremental strategy requirements** (merge, append, delete_insert)
- **Tag-based requirements** (pii, financial, etc.)
- **Package-based requirements** (different packages have different needs)
- **Column validation rules** (required descriptions, data types)
- **Model-level requirements** (descriptions, tests)
- **Flexible exemptions** (regex patterns for excluding models)

Example configuration snippet:
```yaml
required_columns:
  always:
    - name: "created_at"
      data_type: "timestamp"
      description: "Record creation timestamp"

materialization_requirements:
  incremental:
    required_columns:
      - name: "updated_at"
        data_type: "timestamp"

tag_requirements:
  pii:
    required_columns:
      - name: "data_classification"
        data_type: "string"

exclusions:
  fully_exempt:
    - "temp_.*"  # regex pattern
```

### Exit Codes

- `0`: All checks passed
- `1`: Some checks failed (column mismatches or other validation errors)
- `2`: Invalid command line arguments

## Development

This project uses [Hatch](https://hatch.pypa.io/) for project management with [Astral tools](https://astral.sh/) (uv and ruff) for fast dependency management and code quality.

### Setup

```console
# Clone the repository
git clone <repository-url>
cd databuildcheck

# Install Hatch (if not already installed)
uv tool install hatch

# Show available environments and scripts
hatch env show
```

### Available Commands

```console
# Run tests
hatch run test

# Run tests with coverage
hatch run cov

# Lint code
hatch run lint

# Fix linting issues
hatch run lint-fix

# Format code
hatch run format

# Check formatting
hatch run format-check

# Type checking
hatch run types:check
```

### Features

- **Fast dependency management** with uv
- **Code quality** with ruff (linting and formatting)
- **Type checking** with mypy
- **Testing** with pytest and coverage
- **Modern Python packaging** with Hatch

## License

`databuildcheck` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
