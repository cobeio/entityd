"""Local py.test plugin."""

import types
import unittest.mock

import pytest

import entityd.hookspec
import entityd.pm


def pytest_namespace():
    """Add some items to the pytest namespace."""
    return {'Mock': unittest.mock.Mock}


@pytest.fixture
def pm():
    """A PluginManager with the entityd hookspec."""
    return entityd.pm.PluginManager(entityd.hookspec)

