"""Local py.test plugin."""

import types
import unittest.mock

import pytest

import entityd.core
import entityd.hookspec
import entityd.kvstore
import entityd.pm


def pytest_namespace():
    """Add some items to the pytest namespace."""
    return {'Mock': unittest.mock.Mock}


@pytest.fixture
def pm():
    """A PluginManager with the entityd hookspec."""
    return entityd.pm.PluginManager(entityd.hookspec)


@pytest.fixture
def config(pm):
    """An entityd.core.Config instance."""
    return entityd.core.Config(pm, types.SimpleNamespace())


@pytest.fixture
def session(pm, config):
    """An entityd.core.Session instance."""
    return entityd.core.Session(pm, config)


@pytest.fixture
def kvstore(session):
    """Return a kvstore instance registered to the session fixture.

    This creates a KVStore and registers it to the ``session`` fixture.

    """
    kvstore = entityd.kvstore.KVStore(':memory:')
    session.addservice('kvstore', kvstore)
    return kvstore
