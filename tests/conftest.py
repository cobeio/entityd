"""Local py.test plugin."""

import pathlib
import tempfile
import threading
import types
import unittest.mock

import py
import pytest
import zmq.auth

import entityd.core
import entityd.hookspec
import entityd.hostme
import entityd.kvstore
import entityd.monitor
import entityd.pm
import entityd.processme


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


@pytest.yield_fixture
def host_entity_plugin(pm, session, kvstore):  # pylint: disable=unused-argument
    host_plugin = entityd.hostme.HostEntity()
    host_plugin.session = session
    pm.register(host_plugin, 'entityd.hostme.HostEntity')
    host_plugin.entityd_sessionstart(session)
    yield host_plugin
    host_plugin.entityd_sessionfinish()


@pytest.fixture(autouse=True)
def path_health(tmpdir, monkeypatch):
    """Use a temporary file as health marker."""
    monkeypatch.setattr(
        entityd.health,
        '_PATH_HEALTH',
        pathlib.Path(str(tmpdir.join('healthy'))),
    )


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


@pytest.yield_fixture(autouse=True)
def _check_only_one_thread_present_on_tests_completion():
    """Ensure at the end of each test that there is only one thread."""
    yield
    assert len(threading.enumerate()) == 1


@pytest.fixture(autouse=True)
def mock_cpuusage(request):
    """Mock out cpuusage calculation in processme and hostme.

    This fixture is applied to all tests by default, thus simplifying tests
    and avoiding any unnecessary issues of threads remaining alive at the
    end of tests. A function is returned so that the fixture can be used in the
    signature of tests where, by calling the function, the mocking can
    be reverted, e.g. for testing of cpuusage.
    """
    usage = pytest.Mock()
    usage.listen_endpoint = 'inproc://cpuusage'
    cpuusage = entityd.processme.CpuUsage
    entityd.processme.CpuUsage = pytest.Mock(return_value=usage)
    hostcpuusage = entityd.hostme.HostCpuUsage
    entityd.hostme.HostCpuUsage = pytest.Mock(return_value=usage)
    add_cputime_attrs = entityd.hostme.HostEntity._add_cputime_attrs
    entityd.hostme.HostEntity._add_cputime_attrs = lambda self, _: None
    class Reversion:
        @staticmethod
        def revert():
            entityd.processme.CpuUsage = cpuusage
            entityd.hostme.HostCpuUsage = hostcpuusage
            entityd.hostme.HostEntity._add_cputime_attrs = add_cputime_attrs
    request.addfinalizer(Reversion.revert)
    return Reversion


@pytest.yield_fixture
def cluster_entity_plugin(pm, session, kvstore):  # pylint: disable=unused-argument
    cluster_plugin = entityd.kubernetes.cluster.ClusterEntity()
    cluster_plugin.session = session
    @entityd.pm.hookimpl
    def entityd_find_entity(name, attrs):  # pylint: disable=unused-argument
        yield entityd.entityupdate.EntityUpdate('Kubernetes:Cluster',
                                                ueid='a' * 32)
    cluster_plugin.entityd_find_entity = entityd_find_entity
    pm.register(cluster_plugin, 'entityd.kubernetes.cluster.ClusterEntity')
    cluster_plugin.entityd_sessionstart(session)
    yield cluster_plugin
    cluster_plugin.entityd_sessionfinish()
