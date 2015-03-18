import subprocess
import sys
import textwrap

import pytest

import entityd.pm
import entityd.__main__ as main


def delete_modules(mods):
    """Return a function which, when called, will remove the given modules from
    sys.modules.

    :param mods: List of module names to remove.
    """

    def inner():
        for m in mods:
            try:
                del sys.modules[m]
            except KeyError:
                pass
    return inner


def test_main_noplugins(monkeypatch):
    monkeypatch.setattr(main, 'BUILTIN_PLUGIN_NAMES', [])
    assert main.main() is None


def test_main(request, tmpdir, monkeypatch):
    request.addfinalizer(delete_modules(['plugin']))
    plugin = tmpdir.join('plugin.py')
    plugin.write(textwrap.dedent("""\
        import entityd.pm

        @entityd.pm.hookimpl
        def entityd_main(pluginmanager, argv):
            return (pluginmanager, argv)
    """))
    monkeypatch.syspath_prepend(tmpdir)
    pm, argv = main.main(argv=['--foo', 'bar'], plugins=['plugin'])
    assert isinstance(pm, entityd.pm.PluginManager)
    assert argv == ['--foo', 'bar']


def test_main_register_cb(request, tmpdir, monkeypatch):
    request.addfinalizer(delete_modules(['plugin']))
    plugin = tmpdir.join('plugin.py')
    plugin.write('# empty')
    monkeypatch.syspath_prepend(tmpdir)
    monkeypatch.setattr(main, 'plugin_registered_cb', pytest.Mock())
    main.main(argv=[], plugins=['plugin'])
    assert main.plugin_registered_cb.called


def test_main_trace(monkeypatch):
    monkeypatch.setattr(main, 'trace', pytest.Mock())
    main.main(argv=['--trace'], plugins=[])
    assert main.trace.called


def test_main_importerror(request, tmpdir, monkeypatch):
    request.addfinalizer(delete_modules(['plugin_a', 'plugin_b']))
    plugin_a = tmpdir.join('plugin_a.py')
    plugin_a.write("raise Exception('oops')")
    plugin_b = tmpdir.join('plugin_b.py')
    plugin_b.write(textwrap.dedent("""\
        import entityd.pm

        @entityd.pm.hookimpl
        def entityd_main(pluginmanager, argv):
            return (pluginmanager, argv)
    """))
    monkeypatch.syspath_prepend(tmpdir)
    pm, _ = main.main(argv=[], plugins=['plugin_a', 'plugin_b'])
    assert not pm.isregistered('plugin_a')
    assert pm.isregistered('plugin_b')


def test_main_attributeerror(request, tmpdir, monkeypatch):
    request.addfinalizer(delete_modules(['plugin_a', 'plugin_b']))
    plugin_a = tmpdir.join('plugin_a.py')
    plugin_a.write('# empty')
    plugin_b = tmpdir.join('plugin_b.py')
    plugin_b.write(textwrap.dedent("""\
        import entityd.pm

        @entityd.pm.hookimpl
        def entityd_main(pluginmanager, argv):
            return (pluginmanager, argv)
    """))
    monkeypatch.syspath_prepend(tmpdir)
    pm, _ = main.main(argv=[], plugins=['plugin_a:NoClass', 'plugin_b'])
    assert not pm.isregistered('plugin_a')
    assert not pm.isregistered('plugin_a.NoClass')
    assert pm.isregistered('plugin_b')


def test_main_registererror(request, tmpdir, monkeypatch):
    request.addfinalizer(delete_modules(['plugin']))
    plugin = tmpdir.join('plugin.py')
    plugin.write('# empty')
    monkeypatch.syspath_prepend(tmpdir)
    monkeypatch.setattr(entityd.pm.PluginManager, 'register',
                        pytest.Mock(side_effect=Exception('oops')))
    main.main(argv=[], plugins=['plugin'])
    assert entityd.pm.PluginManager.register.called


def test_plugin_class(request, tmpdir, monkeypatch):
    request.addfinalizer(delete_modules(['plugin_a', 'plugin_b']))
    plugin_a = tmpdir.join('plugin_a.py')
    plugin_a.write(textwrap.dedent("""\
        class A:
            pass
    """))
    plugin_b = tmpdir.join('plugin_b.py')
    plugin_b.write(textwrap.dedent("""\
        import entityd.pm

        @entityd.pm.hookimpl
        def entityd_main(pluginmanager, argv):
            return (pluginmanager, argv)
    """))
    monkeypatch.syspath_prepend(tmpdir)
    pm, _ = main.main(argv=[], plugins=['plugin_a:A', 'plugin_b'])
    assert pm.isregistered('plugin_a.A')
    assert pm.isregistered('plugin_b')


def test_plugin_class_with_args(request, tmpdir, monkeypatch):
    request.addfinalizer(delete_modules(['plugin_a', 'plugin_b']))
    plugin_a = tmpdir.join('plugin_a.py')
    plugin_a.write(textwrap.dedent("""\
        class A:
            def __init__(self, arg1):
                pass
    """))
    plugin_b = tmpdir.join('plugin_b.py')
    plugin_b.write(textwrap.dedent("""\
        import entityd.pm

        @entityd.pm.hookimpl
        def entityd_main(pluginmanager, argv):
            return (pluginmanager, argv)
    """))
    monkeypatch.syspath_prepend(tmpdir)
    pm, _ = main.main(argv=[], plugins=['plugin_a:A', 'plugin_b'])
    assert not pm.isregistered('plugin_a')
    assert not pm.isregistered('plugin_a.A')
    assert pm.isregistered('plugin_b')


def test_plugin_registered_cb(tmpdir, pm, monkeypatch):
    source = tmpdir.join('plugin.py')
    source.write('# empty')
    monkeypatch.syspath_prepend(tmpdir)
    mod = source.pyimport(modname='plugin', ensuresyspath=False)
    plugin = pm.register(mod)
    monkeypatch.setattr(pm, 'hooks', pytest.Mock())
    main.plugin_registered_cb(pm, plugin)
    pm.hooks.entityd_plugin_registered.assert_called_with(pluginmanager=pm,
                                                          name='plugin')


def test_plugin_registered_cb_err(tmpdir, pm, monkeypatch):
    source = tmpdir.join('plugin.py')
    source.write('# empty')
    monkeypatch.syspath_prepend(tmpdir)
    mod = source.pyimport(modname='plugin', ensuresyspath=False)
    plugin = pm.register(mod)
    monkeypatch.setattr(pm, 'hooks', pytest.Mock())
    pm.hooks.entityd_plugin_registered.side_effect = Exception('oops')
    main.plugin_registered_cb(pm, plugin)
    assert pm.hooks.entityd_plugin_registered.called


def test_trace(capsys):
    main.trace('hello')
    stdout, _ = capsys.readouterr()
    assert stdout == 'TRACE: hello\n'


def test_script():
    proc = subprocess.Popen([sys.executable, '-m', 'entityd', '--help'])
    proc.wait()
    assert proc.returncode == 0
