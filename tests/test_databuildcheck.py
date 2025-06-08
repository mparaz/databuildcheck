# SPDX-FileCopyrightText: 2025-present Miguel Paraz <mparaz@mparaz.com>
#
# SPDX-License-Identifier: MIT

import databuildcheck


def test_version():
    """Test that the package has a version."""
    assert hasattr(databuildcheck, "__version__")


def test_basic_import():
    """Test that the package can be imported."""
    assert databuildcheck is not None
