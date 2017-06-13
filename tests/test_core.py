import argparse
import sys
import threading
import time

import logbook
import pytest

import entityd.hookspec
import entityd.pm

from entityd import core


@pytest.fixture
def plugin(pm):
    """The entityd.pm.Plugin for entityd.core."""
    return pm.register(core)


def test_entityd_main(pm, hookrec):
    config = pytest.Mock()
    config.args.log_level = logbook.INFO
    class FooPlugin:
        entityd_main = core.entityd_main

        @staticmethod
        @entityd.pm.hookimpl
        def entityd_cmdline_parse(pluginmanager, argv):  # pylint: disable=unused-argument
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
    assert (calls['entityd_sessionstart']['session'] ==
            calls['entityd_mainloop']['session'] ==
            calls['entityd_sessionfinish']['session'])


def test_entityd_exception_in_mainloop(pm, hookrec):
    config = pytest.Mock()
    config.args.log_level = logbook.INFO

    class TestErr(Exception):
        pass

    class FooPlugin:
        entityd_main = core.entityd_main

        @staticmethod
        @entityd.pm.hookimpl
        def entityd_cmdline_parse(pluginmanager, argv):  # pylint: disable=unused-argument
            return config

        @staticmethod
        @entityd.pm.hookimpl
        def entityd_mainloop(session):  # pylint: disable=unused-argument
            raise TestErr()

    pm.register(FooPlugin)
    with pytest.raises(TestErr):
        pm.hooks.entityd_main(pluginmanager=pm, argv=[])

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
    assert (calls['entityd_sessionstart']['session'] ==
            calls['entityd_mainloop']['session'] ==
            calls['entityd_sessionfinish']['session'])


def test_entityd_exception_in_configure(pm, hookrec):
    config = pytest.Mock()
    config.args.log_level = logbook.INFO

    class TestErr(Exception):
        pass

    class FooPlugin:
        entityd_main = core.entityd_main

        @staticmethod
        @entityd.pm.hookimpl
        def entityd_cmdline_parse(pluginmanager, argv):  # pylint: disable=unused-argument
            return config

        @staticmethod
        @entityd.pm.hookimpl
        def entityd_configure(config):  # pylint: disable=unused-argument
            raise TestErr()

    pm.register(FooPlugin)
    with pytest.raises(TestErr):
        pm.hooks.entityd_main(pluginmanager=pm, argv=[])

    assert [c[0] for c in hookrec.calls] == ['entityd_main',
                                             'entityd_cmdline_parse',
                                             'entityd_configure',
                                             'entityd_unconfigure']
    calls = dict(hookrec.calls)
    assert calls['entityd_configure'] == {'config': config}


def test_entityd_exception_in_sessionstart(pm, hookrec):
    config = pytest.Mock()
    config.args.log_level = logbook.INFO

    class TestErr(Exception):
        pass

    class FooPlugin:
        entityd_main = core.entityd_main

        @staticmethod
        @entityd.pm.hookimpl
        def entityd_cmdline_parse(pluginmanager, argv):  # pylint: disable=unused-argument
            return config

        @staticmethod
        @entityd.pm.hookimpl
        def entityd_sessionstart(session):  # pylint: disable=unused-argument
            raise TestErr()

    pm.register(FooPlugin)
    with pytest.raises(TestErr):
        pm.hooks.entityd_main(pluginmanager=pm, argv=[])

    assert [c[0] for c in hookrec.calls] == ['entityd_main',
                                             'entityd_cmdline_parse',
                                             'entityd_configure',
                                             'entityd_sessionstart',
                                             'entityd_sessionfinish',
                                             'entityd_unconfigure']
    calls = dict(hookrec.calls)
    assert calls['entityd_configure'] == {'config': config}
    assert (calls['entityd_sessionstart']['session'] ==
            calls['entityd_sessionfinish']['session'])


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
    assert '--period' in stdout


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

    def test_removeentity(self, pm, config):
        plugin = pm.register(object(), 'foo')
        config.addentity('foo', plugin)
        assert config.entities['foo'] is plugin
        config.removeentity('foo', plugin)
        assert 'foo' not in config.entities.keys()

    def test_remove_unregistered(self, pm, config):
        plugin = pm.register(object(), 'foo')
        with pytest.raises(KeyError):
            config.removeentity('foo', plugin)

    def test_remove_wrong_plugin(self, pm, config):
        plugin_a = pm.register(object(), 'foo')
        plugin_b = pm.register(object(), 'bar')
        config.addentity('foo', plugin_a)
        with pytest.raises(KeyError):
            config.removeentity('foo', plugin_b)

class TestSession:

    @pytest.fixture
    def session(self, pm):
        config = core.Config(pm, argparse.Namespace(period=5))
        return core.Session(pm, config)

    def test_run(self, session, monitor):  # pylint: disable=unused-argument
        session.svc.monitor = pytest.Mock()
        session._shutdown = pytest.Mock()
        session._shutdown.is_set.side_effect = [False, False, True]
        session.run()
        assert session.svc.monitor.collect_entities.called

    def test_shutdown(self, session):
        session.svc.monitor = pytest.Mock()
        thread = threading.Thread(target=session.run)
        thread.start()
        t = 0
        while not session.svc.monitor.collect_entities.called:
            time.sleep(0.001)
            t += 0.001
            if t > 3:
                raise AssertionError('.run() failed to start')
        session.shutdown()
        thread.join(3)
        assert not thread.is_alive()

    def test_addservice(self, session):
        session.addservice('list', list)
        assert session.svc.list((0, 1)) == [0, 1]

    def test_addservice_duplicate(self, session):
        session.addservice('list', list)
        with pytest.raises(KeyError):
            session.addservice('list', tuple)
        assert session.svc.list((0, 1)) == [0, 1]
