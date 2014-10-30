"""Local py.test plugin."""

import unittest.mock


def pytest_namespace():
    """Add some items to the pytest namespace."""
    return {'Mock': unittest.mock.Mock}
