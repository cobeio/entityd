import argparse
import sys
import threading
import time

import pytest

import entityd.hookspec
import entityd.pm

from entityd import core


@pytest.fixture
def plugin(pm):
    """The entityd.pm.Plugin for entityd.core."""
    return pm.register(core)


class HookRecorder:
    """Recorder for hook calls.

    Each hook call performed is recorded in the ``calls`` attribute as
    a tuple of the hook name and dictiontary of it's keyword
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


def test_entityd_main(pm, hookrec):
    config = pytest.Mock()
    class FooPlugin:
        entityd_main = core.entityd_main

        @entityd.pm.hookimpl
        def entityd_cmdline_parse(pluginmanager, argv):  # pylint: disable=E0213,W0613
            return config

    pm.register(FooPlugin)
    ret = pm.hooks.entityd_main(pluginmanager=pm, argv=[])
    assert ret == 0
    assert [c[0] for c in hookrec.calls] == ['entityd_main',
                                             'entityd_cmdline_parse',
                                             'entityd_configure',
                                             'entityd_sessionstart',
                                             'entityd_mainloop',
                                             'entityd_sessionfinish',
                                             'entityd_unconfigure']
    calls = dict(hookrec.calls)
    assert calls['entityd_configure'] == {'config': config}
    assert isinstance(calls['entityd_sessionstart']['session'], core.Session)


def test_entityd_cmdline_parse(pm, hookrec):
    config = core.entityd_cmdline_parse(pm, [])
    assert isinstance(config, core.Config)
    calls = dict(hookrec.calls)
    assert 'parser' in calls['entityd_addoption']


def test_entityd_cmdline_parse_help(pm, capsys):
    with pytest.raises(SystemExit):
        core.entityd_cmdline_parse(pm, ['--help'])
    stdout, stderr = capsys.readouterr()
    print(stderr, file=sys.stderr)
    assert 'entityd' in stdout
    assert '--help' in stdout


def test_entityd_addoption(capsys):
    parser = argparse.ArgumentParser()
    core.entityd_addoption(parser)
    with pytest.raises(SystemExit):
        parser.parse_args(['--help'])
    stdout, stderr = capsys.readouterr()
    print(stderr, file=sys.stderr)
    assert '--version' in stdout
    assert '--log-level' in stdout
    assert '--trace' in stdout


def test_entityd_mainloop():
    session = pytest.Mock()
    core.entityd_mainloop(session)
    assert session.run.called


def test_entityd_mainloop_interrupt():
    session = pytest.Mock()
    session.run.side_effect = KeyboardInterrupt
    core.entityd_mainloop(session)


class TestConfig:

    @pytest.fixture
    def config(self, pm):
        return core.Config(pm, argparse.Namespace())

    def test_addentity(self, pm, config):
        plugin = pm.register(object(), 'foo')
        config.addentity('foo', plugin)
        assert config.entities['foo'] is plugin

    def test_addentity_duplicate(self, pm, config):
        plugin_a = pm.register(object(), 'foo')
        plugin_b = pm.register(object(), 'bar')
        config.addentity('foo', plugin_a)
        with pytest.raises(KeyError):
            config.addentity('foo', plugin_b)


class TestSession:

    @pytest.fixture
    def session(self, pm):
        config = core.Config(pm, argparse.Namespace())
        return core.Session(pm, config)

    def test_run(self, session):
        session.collect_entities = pytest.Mock()
        session._shutdown = pytest.Mock()
        session._shutdown.is_set.side_effect = [False, True]
        session.run()
        assert session.collect_entities.called

    def test_shutdown(self, session):
        session.collect_entities = pytest.Mock()
        thread = threading.Thread(target=session.run)
        thread.start()
        t = 0
        while not session.collect_entities.called:
            time.sleep(0.001)
            t += 0.001
            if t > 3:
                raise AssertionError('.run() failed to start')
        session.shutdown()
        thread.join(3)
        assert not thread.is_alive()

    def test_collect_entities(self, pm, session, hookrec):
        class FooPlugin:
            @entityd.pm.hookimpl
            def entityd_find_entity(self, name, attrs):
                return [(name, attrs)]
        plugin = pm.register(FooPlugin(), 'foo')
        session.config.addentity('foo', plugin)
        session.collect_entities()
        send_entity = dict(hookrec.calls)['entityd_send_entity']
        assert send_entity == {'session': session, 'entity': ('foo', None)}

    def test_collect_entities_none_registered(self, session, hookrec):
        session.collect_entities()
        calls = dict(hookrec.calls)
        assert 'entityd_send_entity' not in calls

    def test_collect_entities_noent(self, pm, session, hookrec):
        class FooPlugin:
            @entityd.pm.hookimpl
            def entityd_find_entity(self):
                return []
        plugin = pm.register(FooPlugin(), 'foo')
        session.config.addentity('foo', plugin)
        session.collect_entities()
        calls = dict(hookrec.calls)
        assert 'entityd_send_entity' not in calls
