"""Local py.test plugin."""

import tempfile
import types
import unittest.mock

import py
import pytest
import zmq.auth

import entityd.core
import entityd.hookspec
import entityd.kvstore
import entityd.monitor
import entityd.pm


def pytest_namespace():
    """Add some items to the pytest namespace."""
    return {
        'Mock': unittest.mock.Mock,
        'MagicMock': unittest.mock.MagicMock,
    }


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
def monitor(session, kvstore):  # pylint: disable=unused-argument
    """An entityd.monitor.Monitor instance."""
    monitor = entityd.monitor.Monitor()
    monitor.entityd_sessionstart(session)


@pytest.fixture
def kvstore(session):
    """Return a kvstore instance registered to the session fixture.

    This creates a KVStore and registers it to the ``session`` fixture.

    """
    kvstore = entityd.kvstore.KVStore(':memory:')
    session.addservice('kvstore', kvstore)
    return kvstore


@pytest.fixture
def host_entity_plugin(pm, session, kvstore):  # pylint: disable=unused-argument
    host_plugin = entityd.hostme.HostEntity()
    host_plugin.session = session
    pm.register(host_plugin, 'entityd.hostme.HostEntity')
    return host_plugin


class HookRecorder:
    """Recorder for hook calls.

    Each hook call performed is recorded in the ``calls`` attribute as
    a tuple of the hook name and dictionary of it's keyword
    arguments.

    Only hooks already present when the recorder is applied will be
    recorded.  If ``.addhooks()`` is called later any new hooks will
    not be recorded.

    """

    def __init__(self, pm):
        self._pm = pm
        self.calls = []
        self._orig_call = entityd.pm.HookCaller.__call__

        def callwrapper(inst, **kwargs):
            self.calls.append((inst.name, kwargs))
            return self._orig_call(inst, **kwargs)

        entityd.pm.HookCaller.__call__ = callwrapper

    def close(self):
        """Undo the hookwrapper."""
        entityd.pm.HookCaller.__call__ = self._orig_call

    def __enter__(self):
        pass

    def __exit__(self, exc, val, tb):
        self.close()


@pytest.fixture
def hookrec(request, pm):
    """Return a HookRecorder attached to the PluginManager instance.

    This attached a HookRecorder to the PluginManager instance of the
    ``pm`` fixture.

    """
    rec = HookRecorder(pm)
    request.addfinalizer(rec.close)
    return rec


@pytest.fixture(scope='session')
def certificates(request):
    """Generate auth certificates that can be used for testing.

    Also copy the public key for modeld to the entityd keys directory, and
    vice-versa.
    """
    conf_dir = py.path.local(tempfile.mkdtemp())
    request.addfinalizer(lambda: conf_dir.remove(rec=1))
    modeld_keys = conf_dir.ensure('modeld/keys', dir=True)
    entityd_keys = conf_dir.ensure('entityd/keys', dir=True)
    modeld_public, _ = zmq.auth.create_certificates(
        modeld_keys.strpath, 'modeld')
    entityd_public, _ = zmq.auth.create_certificates(
        entityd_keys.strpath, 'entityd')
    py.path.local(modeld_public).copy(entityd_keys)
    py.path.local(entityd_public).copy(modeld_keys)

    return conf_dir
