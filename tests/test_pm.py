import types

import pytest

import entityd.pm


def test_hookdef_plain():
    @entityd.pm.hookdef
    def foo(param):
        return param

    assert foo.__name__ == foo.pm_hookdef['name'] == 'foo'
    assert foo.pm_hookdef['firstresult'] is False
    assert foo(1) == 1


def test_hookdef_param():
    @entityd.pm.hookdef(firstresult=True)
    def foo(param):
        return param

    assert foo.__name__ == foo.pm_hookdef['name'] == 'foo'
    assert foo.pm_hookdef['firstresult'] is True
    assert foo(1) == 1


def test_hookimp_plain():
    @entityd.pm.hookimpl
    def foo(param):
        return param

    assert foo.__name__ == foo.pm_hookimpl['name'] == 'foo'
    assert foo.pm_hookimpl['before'] is None
    assert foo.pm_hookimpl['after'] is None
    assert foo(1) == 1


def test_hookimp_param():
    @entityd.pm.hookimpl(before='spam', after=['ham', 'eggs'])
    def foo(param):
        return param

    assert foo.__name__ == foo.pm_hookimpl['name'] == 'foo'
    assert foo.pm_hookimpl['before'] == 'spam'
    assert foo.pm_hookimpl['after'] == ['ham', 'eggs']
    assert foo(1) == 1


class TestPluginManager:

    @pytest.fixture
    def pm(self):
        """An entityd.pm.PluginManager instance"""
        @entityd.pm.hookdef
        def my_hook(param):     # pylint: disable=unused-argument
            pass
        hookspec = types.ModuleType('hookspec')
        hookspec.my_hook = my_hook
        return entityd.pm.PluginManager(hookspec)

    @pytest.fixture(params=['module', 'class'])
    def plugin_obj(self, request):
        if request.param == 'module':
            @entityd.pm.hookimpl
            def my_hook(param):
                return param
            obj = types.ModuleType('myplugin')
            obj.my_hook = my_hook
        else:
            class MyClassPlugin:
                @entityd.pm.hookimpl
                def my_hook(self, param):
                    return param
            obj = MyClassPlugin()
            obj.__name__ = 'myplugin'  # pylint: disable=W0201
        return obj

    def test_register_retval(self, pm, plugin_obj):
        plugin = pm.register(plugin_obj)
        assert isinstance(plugin, entityd.pm.Plugin)

    def test_register_name(self, pm, plugin_obj):
        plugin = pm.register(plugin_obj, 'foo')
        assert plugin.name == 'foo'

    def test_register_noname(self, pm, plugin_obj):
        del plugin_obj.__name__
        with pytest.raises(ValueError):
            pm.register(plugin_obj)

    def test_register_index(self, pm, plugin_obj):
        plugin_a = pm.register(plugin_obj)
        plugin_b = pm.register(plugin_obj, 'foo')
        assert plugin_a.index < plugin_b.index

    def test_register_duplicate(self, pm, plugin_obj):
        pm.register(plugin_obj)
        with pytest.raises(ValueError):
            pm.register(plugin_obj)

    def test_unregister_obj(self, pm, plugin_obj):
        pm.register(plugin_obj)
        assert pm.isregistered(plugin_obj)
        pm.unregister(plugin_obj)
        assert not pm.isregistered(plugin_obj)

    def test_unregister_plugin(self, pm, plugin_obj):
        plugin = pm.register(plugin_obj)
        assert pm.isregistered(plugin_obj)
        assert pm.isregistered(plugin)
        pm.unregister(plugin)
        assert not pm.isregistered(plugin_obj)
        assert not pm.isregistered(plugin)

    def test_unregister_name(self, pm, plugin_obj):
        plugin = pm.register(plugin_obj)
        assert pm.isregistered(plugin_obj)
        assert pm.isregistered(plugin.name)
        pm.unregister(plugin.name)
        assert not pm.isregistered(plugin_obj)
        assert not pm.isregistered(plugin.name)

    def test_register_cb_set(self, pm, plugin_obj):
        l = []
        pm.register_callback = lambda p: l.append(p)
        plugin = pm.register(plugin_obj)
        assert len(l) == 1
        assert l[0] is plugin

    def test_register_cb_get(self, pm):
        assert pm.register_callback is None
        cb = lambda p: None
        pm.register_callback = cb
        assert pm.register_callback is cb

    def test_trace_unset(self, pm):
        pm._trace('hello')
        assert pm.tracer_cb is None

    def test_trace_set(self, pm):
        l = []
        pm.tracer_cb = lambda m: l.append(m)
        pm._trace('hello')
        assert l == ['hello']

    def test_trace_del(self, pm):
        l = []
        pm.tracer_cb = lambda m: l.append(m)
        pm.tracer_cb = None
        pm._trace('hello')
        assert not l

    def test_isregistered(self, pm, plugin_obj):
        plugin = pm.register(plugin_obj)
        assert pm.isregistered(plugin_obj)
        assert pm.isregistered(plugin)
        assert pm.isregistered(plugin.name)

    def test_hooks(self, pm):
        assert isinstance(pm.hooks.my_hook, entityd.pm.HookCaller)

    def test_addhooks(self, pm):
        @entityd.pm.hookdef
        def spam(param):        # pylint: disable=unused-argument
            pass
        hookspec = types.ModuleType('hookspec')
        hookspec.spam = spam
        pm.addhooks(hookspec)
        assert isinstance(pm.hooks.spam, entityd.pm.HookCaller)


class TestPlugin:

    @pytest.fixture
    def plugin_obj(self):
        @entityd.pm.hookimpl
        def my_hook(param):
            return param
        obj = types.ModuleType('myplugin')
        obj.my_hook = my_hook
        return obj

    def test_attrs(self, plugin_obj):
        plugin = entityd.pm.Plugin(plugin_obj, 'foo', 5)
        assert plugin.obj is plugin_obj
        assert plugin.name == 'foo'
        assert plugin.index == 5


class TestHookImpl:

    @pytest.fixture
    def plugin(self):
        @entityd.pm.hookimpl
        def my_hook(arg0, arg1):  # pylint: disable=unused-argument
            pass
        obj = types.ModuleType('myplugin')
        obj.my_hook = my_hook
        return entityd.pm.Plugin(obj, 'myplugin', 0)

    def test_attrs(self, plugin):
        hook = entityd.pm.HookImpl(plugin.obj.my_hook, plugin)
        assert hook.routine is plugin.obj.my_hook
        assert hook.plugin is plugin
        assert hook.name == 'my_hook'
        assert hook.before == set()
        assert hook.after == set()

    @pytest.mark.parametrize('arg',
                             [None, 'spam', ['spam', 'ham']],
                             ids=['None', 'str', 'seq'])
    def test_before_after(self, arg):
        @entityd.pm.hookimpl(before=arg, after=arg)
        def my_hook(param):
            return param
        obj = types.ModuleType('myplugin')
        obj.my_hook = my_hook
        plugin = entityd.pm.Plugin(obj, 'myplugin', 0)
        hook = entityd.pm.HookImpl(plugin.obj.my_hook, plugin)
        if arg is None:
            assert hook.before == set()
            assert hook.after == set()
        elif arg == 'spam':
            assert hook.before == set(['spam'])
            assert hook.after == set(['spam'])
        else:
            assert hook.before == set(['spam', 'ham'])
            assert hook.after == set(['spam', 'ham'])

    def test_before_after_badtype(self):
        @entityd.pm.hookimpl(before=42, after=42)
        def my_hook(param):
            return param
        obj = types.ModuleType('myplugin')
        obj.my_hook = my_hook
        with pytest.raises(TypeError):
            plugin = entityd.pm.Plugin(obj, 'myplugin', 0)
            entityd.pm.HookImpl(plugin.obj.my_hook, plugin)

    def test_argnames(self, plugin):
        hook = entityd.pm.HookImpl(plugin.obj.my_hook, plugin)
        assert list(hook.argnames()) == ['arg0', 'arg1']

    def test_repr(self, plugin):
        hook = entityd.pm.HookImpl(plugin.obj.my_hook, plugin)
        assert 'HookImpl' in repr(hook)
        assert 'myplugin' in repr(hook)
        assert 'my_hook' in repr(hook)


class TestHookRelay:

    @pytest.fixture
    def relay(self):
        return entityd.pm.HookRelay(lambda x: None)

    @pytest.fixture
    def hookspec(self):
        @entityd.pm.hookdef
        def hook_a():
            pass
        @entityd.pm.hookdef
        def hook_b(spam, ham):  # pylint: disable=unused-argument
            pass
        def dummy():
            pass
        mod = types.ModuleType('hookspec')
        mod.hook_a = hook_a
        mod.hook_b = hook_b
        mod.dummy = dummy
        return mod

    @pytest.fixture
    def plugin(self):
        @entityd.pm.hookimpl
        def hook_a():
            pass
        def dummy():
            pass
        mod = types.ModuleType('my_plugin')
        mod.hook_a = hook_a
        mod.dummy = dummy
        return entityd.pm.Plugin(mod, 'my_plugin', 0)

    def test_addhooks(self, relay, hookspec):
        relay.addhooks(hookspec)
        assert isinstance(relay.hooks.hook_a, entityd.pm.HookCaller)
        assert isinstance(relay.hooks.hook_b, entityd.pm.HookCaller)
        assert not hasattr(relay.hooks, 'dummy')

    def test_addhooks_none(self, relay, hookspec):
        del hookspec.hook_a
        del hookspec.hook_b
        with pytest.raises(ValueError):
            relay.addhooks(hookspec)

    def test_addhooks_dup(self, relay, hookspec):
        relay.addhooks(hookspec)
        with pytest.raises(ValueError):
            relay.addhooks(hookspec)

    def test_addplugin(self, relay, hookspec, plugin, monkeypatch):
        relay.addhooks(hookspec)
        addimpl = pytest.Mock()
        monkeypatch.setattr(relay.hooks.hook_a, 'addimpl', addimpl)
        relay.addplugin(plugin)
        assert addimpl.called
        print(addimpl.call_args)
        impl = addimpl.call_args[0][0]
        assert isinstance(impl, entityd.pm.HookImpl)

    def test_addplugin_unknown_hook(self, relay, plugin):
        with pytest.raises(ValueError):
            relay.addplugin(plugin)

    def test_removeplugin(self, relay, hookspec, plugin):
        relay.addhooks(hookspec)
        relay.hooks.hook_a.removeimpl = pytest.Mock()
        relay.removeplugin(plugin)
        impl = plugin.hooks[0]
        relay.hooks.hook_a.removeimpl.assert_called_once_with(impl)


class TestHookCaller:

    @pytest.fixture
    def caller(self):
        @entityd.pm.hookdef
        def my_hook(spam, ham):  # pylint: disable=unused-argument
            pass
        return entityd.pm.HookCaller(my_hook, lambda m: None)

    @pytest.fixture
    def caller_firstresult(self):
        @entityd.pm.hookdef(firstresult=True)
        def my_hook(spam, ham):  # pylint: disable=unused-argument
            pass
        return entityd.pm.HookCaller(my_hook, lambda m: None)

    @pytest.fixture
    def impl_spam(self):
        @entityd.pm.hookimpl
        def my_hook(spam):
            return spam
        mod = types.ModuleType('spamplugin')
        mod.my_hook = my_hook
        plugin = entityd.pm.Plugin(mod, 'spamplugin', 0)
        return entityd.pm.HookImpl(my_hook, plugin)

    @pytest.fixture
    def impl_ham(self):
        @entityd.pm.hookimpl
        def my_hook(ham):
            return ham
        mod = types.ModuleType('hamplugin')
        mod.my_hook = my_hook
        plugin = entityd.pm.Plugin(mod, 'hamplugin', 1)
        return entityd.pm.HookImpl(my_hook, plugin)

    @pytest.fixture
    def impl_badarg(self):
        @entityd.pm.hookimpl
        def my_hook(badarg):
            return badarg
        mod = types.ModuleType('badargplugin')
        mod.my_hook = my_hook
        plugin = entityd.pm.Plugin(mod, 'badargplugin', 2)
        return entityd.pm.HookImpl(my_hook, plugin)

    @pytest.fixture
    def impl_before(self):
        @entityd.pm.hookimpl(before=['spamplugin', 'hamplugin'])
        def my_hook():
            return 'before'
        mod = types.ModuleType('beforeplugin')
        mod.my_hook = my_hook
        plugin = entityd.pm.Plugin(mod, 'beforeplugin', 3)
        return entityd.pm.HookImpl(my_hook, plugin)

    @pytest.fixture
    def impl_after(self):
        @entityd.pm.hookimpl(after=['spamplugin', 'hamplugin'])
        def my_hook():
            return 'after'
        mod = types.ModuleType('afterplugin')
        mod.my_hook = my_hook
        plugin = entityd.pm.Plugin(mod, 'afterplugin', 4)
        return entityd.pm.HookImpl(my_hook, plugin)

    @pytest.fixture
    def impl_between(self):
        @entityd.pm.hookimpl(after='spamplugin', before='hamplugin')
        def my_hook():
            return 'between'
        mod = types.ModuleType('betweenplugin')
        mod.my_hook = my_hook
        plugin = entityd.pm.Plugin(mod, 'betweenplugin', 5)
        return entityd.pm.HookImpl(my_hook, plugin)

    @pytest.fixture
    def impl_noval(self):
        @entityd.pm.hookimpl
        def my_hook():
            return None
        mod = types.ModuleType('novalplugin')
        mod.my_hook = my_hook
        plugin = entityd.pm.Plugin(mod, 'novalplugin', 6)
        return entityd.pm.HookImpl(my_hook, plugin)

    @pytest.fixture
    def impl_meth(self):
        class ThePlugin:
            @entityd.pm.hookimpl
            def my_hook(self, spam, ham):
                return spam, ham
        plugin = entityd.pm.Plugin(ThePlugin(), 'clsplugin', 7)
        return entityd.pm.HookImpl(plugin.obj.my_hook, plugin)

    def test_attrs(self, caller):
        assert caller.name == 'my_hook'
        assert not caller.firstresult

    def test_attrs_firstresult(self, caller_firstresult):
        assert caller_firstresult.name == 'my_hook'
        assert caller_firstresult.firstresult

    def test_incorrectly_ordered_before(self, caller, impl_spam, impl_before):
        assert caller.correctly_ordered([impl_spam, impl_before]) is False

    def test_incorrectly_ordered_after(self, caller, impl_spam, impl_after):
        assert caller.correctly_ordered([impl_after, impl_spam]) is False

    def test_correctly_ordered_before(self, caller, impl_spam, impl_before):
        assert caller.correctly_ordered([impl_before, impl_spam]) is True

    def test_correctly_ordered_after(self, caller, impl_spam, impl_after):
        assert caller.correctly_ordered([impl_spam, impl_after]) is True

    def test_addimpl_single(self, caller, impl_spam):
        caller.addimpl(impl_spam)
        assert caller._hooks == [impl_spam]

    def test_addimpl_noval(self, caller, impl_noval):
        caller.addimpl(impl_noval)
        assert caller._hooks == [impl_noval]

    def test_addimpl_already_dup(self, caller, impl_spam):
        caller.addimpl(impl_spam)
        with pytest.raises(ValueError):
            caller.addimpl(impl_spam)

    def test_addimpl_badarg(self, caller, impl_badarg):
        with pytest.raises(TypeError):
            caller.addimpl(impl_badarg)

    def test_addimpl_plugin_load_order(self, caller, impl_spam, impl_ham):
        caller.addimpl(impl_spam)
        caller.addimpl(impl_ham)
        assert caller._hooks == [impl_spam, impl_ham]

    def test_addimpl_before(self, caller, impl_spam, impl_ham, impl_before):
        caller.addimpl(impl_spam)
        caller.addimpl(impl_ham)
        caller.addimpl(impl_before)
        assert caller._hooks == [impl_before, impl_spam, impl_ham]

    def test_addimpl_after(self, caller, impl_spam, impl_ham, impl_after):
        caller.addimpl(impl_spam)
        caller.addimpl(impl_ham)
        caller.addimpl(impl_after)
        assert caller._hooks == [impl_spam, impl_ham, impl_after]

    def test_addimpl_between(self, caller, impl_spam, impl_ham, impl_between):
        caller.addimpl(impl_spam)
        caller.addimpl(impl_ham)
        caller.addimpl(impl_between)
        assert caller._hooks == [impl_spam, impl_between, impl_ham]

    def test_addimpl_all(self, caller, impl_spam, impl_ham,
                         impl_before, impl_after, impl_between):
        caller.addimpl(impl_spam)
        caller.addimpl(impl_ham)
        caller.addimpl(impl_before)
        caller.addimpl(impl_after)
        caller.addimpl(impl_between)
        assert caller._hooks == [impl_before, impl_spam,
                                 impl_between, impl_ham, impl_after]

    def test_addimpl_before_missing(self, caller, impl_before):
        caller.addimpl(impl_before)
        assert caller._hooks == [impl_before]

    def test_addimpl_after_missing(self, caller, impl_after):
        caller.addimpl(impl_after)
        assert caller._hooks == [impl_after]

    def test_addimpl_between_missing(self, caller, impl_between):
        caller.addimpl(impl_between)
        assert caller._hooks == [impl_between]

    def test_addimpl_impossible(self, caller):
        @entityd.pm.hookimpl(before='b')
        def my_hook(ham):
            return ham
        mod = types.ModuleType('a')
        mod.my_hook = my_hook
        plugin_a = entityd.pm.Plugin(mod, 'a', 0)
        hook_a = entityd.pm.HookImpl(my_hook, plugin_a)

        @entityd.pm.hookimpl(before='a')
        def my_hook(ham):       # pylint: disable=function-redefined
            return ham
        mod = types.ModuleType('b')
        mod.my_hook = my_hook
        plugin_b = entityd.pm.Plugin(mod, 'b', 1)
        hook_b = entityd.pm.HookImpl(my_hook, plugin_b)

        caller.addimpl(hook_a)
        with pytest.raises(ValueError):
            caller.addimpl(hook_b)

    def test_addimpl_after_self(self, caller):
        @entityd.pm.hookimpl(after='a')
        def my_hook(ham):
            return ham
        mod = types.ModuleType('a')
        mod.my_hook = my_hook
        plugin_a = entityd.pm.Plugin(mod, 'a', 0)
        hook_a = entityd.pm.HookImpl(my_hook, plugin_a)
        with pytest.raises(ValueError):
            caller.addimpl(hook_a)

    def test_addimpl_before_self(self, caller):
        @entityd.pm.hookimpl(before='a')
        def my_hook(ham):
            return ham
        mod = types.ModuleType('a')
        mod.my_hook = my_hook
        plugin_a = entityd.pm.Plugin(mod, 'a', 0)
        hook_a = entityd.pm.HookImpl(my_hook, plugin_a)
        with pytest.raises(ValueError):
            caller.addimpl(hook_a)

    def test_addimpl_before_and_after(self, caller, impl_spam):
        @entityd.pm.hookimpl(before='spamplugin', after='spamplugin')
        def my_hook(ham):
            return ham
        mod = types.ModuleType('a')
        mod.my_hook = my_hook
        plugin_a = entityd.pm.Plugin(mod, 'a', 0)
        hook_a = entityd.pm.HookImpl(my_hook, plugin_a)
        caller.addimpl(impl_spam)
        with pytest.raises(ValueError):
            caller.addimpl(hook_a)

    def test_removeimpl(self, caller, impl_spam):
        caller._hooks = [impl_spam]
        caller.removeimpl(impl_spam)
        assert caller._hooks == []

    def test_removeimpl_multi(self, caller, impl_spam, impl_ham):
        caller._hooks = [impl_spam, impl_ham]
        caller.removeimpl(impl_spam)
        assert caller._hooks == [impl_ham]

    def test_removeimpl_multi_order(self, caller,
                                    impl_spam, impl_ham, impl_before):
        caller._hooks = [impl_before, impl_spam, impl_ham]
        caller.removeimpl(impl_spam)
        assert caller._hooks == [impl_before, impl_ham]

    def test_removeimpl_nonexist(self, caller, impl_spam):
        with pytest.raises(ValueError):
            caller.removeimpl(impl_spam)

    def test_call_positional_args(self, caller):
        with pytest.raises(TypeError):
            caller(42, 43)

    def test_call_extra_arg(self, caller):
        with pytest.raises(TypeError):
            caller(spam=42, ham=3, foo=1)

    def test_call_no_hooks(self, caller):
        assert caller(spam=42, ham=3) == []

    def test_call_single_hook(self, caller, impl_spam):
        caller._hooks = [impl_spam]
        assert caller(spam=42, ham=3) == [42]

    def test_call_multiple_hooks(self, caller, impl_spam, impl_ham):
        caller._hooks = [impl_spam, impl_ham]
        assert caller(spam=42, ham=3) == [42, 3]

    def test_call_firstresult(self, caller_firstresult, impl_spam, impl_ham):
        caller_firstresult._hooks = [impl_spam, impl_ham]
        assert caller_firstresult(spam=42, ham=3) == 42

    def test_call_firstresult_noval(self, caller_firstresult, impl_noval):
        caller_firstresult._hooks = [impl_noval]
        assert caller_firstresult(spam=42, ham=3) is None

    def test_call_noval(self, caller, impl_noval):
        caller._hooks = [impl_noval]
        assert caller(spam=42, ham=3) == []

    def test_call_noval_spam(self, caller, impl_noval, impl_spam):
        caller._hooks = [impl_noval, impl_spam]
        assert caller(spam=42, ham=3) == [42]

    def test_call_impl_meth(self, caller, impl_meth):
        caller._hooks = [impl_meth]
        assert caller(spam=42, ham=3) == [(42, 3)]

    def test_call_exception(self, caller):
        class MyException(Exception):
            pass
        class ThePlugin:
            @entityd.pm.hookimpl
            def my_hook(self):
                raise MyException('oops')
        plugin = entityd.pm.Plugin(ThePlugin(), 'theplugin', 0)
        impl = entityd.pm.HookImpl(plugin.obj.my_hook, plugin)
        caller.addimpl(impl)
        with pytest.raises(MyException):
            caller(spam=1, ham=2)

    def test_cls_hookspec(self, impl_spam):
        # This is a quite weird thing to support, probably not necessary.
        class HookSpec:
            @entityd.pm.hookdef
            def my_hook(self, spam, ham):  # pylint: disable=unused-argument
                pass
        caller = entityd.pm.HookCaller(HookSpec().my_hook, lambda m: None)
        caller.addimpl(impl_spam)
        assert caller(spam=42, ham=3) == [42]
