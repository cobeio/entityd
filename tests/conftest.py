"""Local py.test plugin."""

import types
import unittest.mock

import pytest

import entityd.pm


def pytest_namespace():
    """Add some items to the pytest namespace."""
    return {'Mock': unittest.mock.Mock}


@pytest.fixture
def pm():
    """An entityd.pm.PluginManager instance"""

    @entityd.pm.hookdef
    def my_hook(param):  # pylint: disable=unused-argument
        pass

    hookspec = types.ModuleType('hookspec')
    hookspec.my_hook = my_hook
    return entityd.pm.PluginManager(hookspec)
