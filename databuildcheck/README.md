# databuildcheck

[![PyPI - Version](https://img.shields.io/pypi/v/databuildcheck.svg)](https://pypi.org/project/databuildcheck)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/databuildcheck.svg)](https://pypi.org/project/databuildcheck)

-----

## Table of Contents

- [Installation](#installation)
- [Development](#development)
- [License](#license)

## Installation

```console
pip install databuildcheck
```

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
