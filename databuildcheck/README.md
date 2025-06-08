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

#### Key Features
- **CTE Handling**: Correctly handles complex SQL with CTEs by excluding CTE names from table reference validation
- **Environment Flexibility**: Database and schema substitutions allow the same manifest to work across different environments
- **Comprehensive Coverage**: Checks both column consistency and table reference validity

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
