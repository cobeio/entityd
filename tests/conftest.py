"""Local py.test plugin."""

import unittest.mock

import pytest

import entityd.hookspec


def pytest_namespace():
    """Add some items to the pytest namespace."""
    return {'Mock': unittest.mock.Mock}


@pytest.fixture
def pm():
    """A PluginManager with the entityd hookspec."""
    return entityd.pm.PluginManager(entityd.hookspec)
