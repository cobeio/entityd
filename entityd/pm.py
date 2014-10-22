"""PluginManager Infrastructure."""

import collections
import inspect
import itertools
import types


def hookdef(*func, firstresult=False):
    """Declare decorated function as a hook definition

    If the func argument is given then it must be a callable and this
    function is a decorator which marks the callable as a hook
    definition.  Otherwise it must be called with keyword arguments
    only and this function will return a decorator to mark a callable
    as a hook definition, but with the added details.

    :firstresult: Only the first hook will be used.  If this hook
       returns ``None`` then the next hook will be used etc.

    """
    detail = {'firstresult': firstresult}
    if func:
        assert len(func) == 1
        assert inspect.isroutine(func[0])
        func = func[0]
        detail.update(name=func.__name__)
        func.pm_hookdef = detail
        return func
    else:
        def _hookdef(func):
            detail.update(name=func.__name__)
            func.pm_hookdef = detail
            return func
        return _hookdef


def hookimpl(*func, before=None, after=None):
    """Declare a function or method as a hook implementionation.

    If the func argument is given then it must be a callable and this
    function is a decorator wich marsk the callable as a hook
    implementation.  Otherwise it must be called with keyword
    arguments only and this function will return a decorator to mark a
    callable as a hook implementation, but with details.

    :before: Mark that this hook must be called before the corresponding hooks
       from the named plugins.  This can be either a string or a list
       of strings.

    :after: Mark that this hook must be called after the corresponding
       hooks from named plugins.  This can be either a string or a
       list of strings.

    If the hook implementation from a plugin conflicts with already
    loaded plugins an exception will be raised during loading of the
    plugin.

    """
    detail = {'before': before,
              'after': after}
    if func:
        assert len(func) == 1
        assert inspect.isroutine(func[0])
        func = func[0]
        detail.update(name=func.__name__)
        func.pm_hookimpl = detail
        return func
    else:
        def _hookimpl(func):
            detail.update(name=func.__name__)
            func.pm_hookimpl = detail
            return func
        return _hookimpl


class PluginManager:
    """The plugin manager."""

    def __init__(self, hookspec=None):
        self._plugins = {}
        self._hookrelay = HookRelay(hookspec)
        self.hooks = self._hookrelay.hooks
        self._plugin_count = itertools.count()
        self._register_cb = None

    def register(self, plugin, name=None):
        """Register a new plugin.

        :param plugin: The object (module or class) which contains the
           hook routines.

        :param name: The name to give to the function.  If not given
           this will try to use ``plugin.__name__`` if it exists,
           otherwise a ValueError is raised.

        """
        index = next(self._plugin_count)
        if not name:
            try:
                name = plugin.__name__
            except AttributeError:
                raise ValueError('Missing plugin name')
        plugin = Plugin(plugin, name, index)
        if self.isregistered(plugin):
            raise ValueError('Plugin already registered: {}'.format(name))
        self._hookrelay.add_plugin(plugin)
        self._plugins[name] = plugin
        if self._register_cb:
            self._register_cb(plugin)

    def unregister(self, plugin):
        # Plugin-or-name-or-obj
        pass

    @property
    def register_callback(self):
        """Callback which will be called when a plugin is registered.

        Assign a callable to this which will be called with a single
        argument which is the Plugin instance of the newly registered
        plugin.

        Assign None to disable.

        """
        return self._register_cb

    @register_callback.setter
    def register_callback(self, cb):
        self._register_cb = cb

    def isregistered(self, plugin):
        # Plugin-or-name-or-obj
        pass

    def getplugin(self, name):
        pass

    def addhooks(self, hookspec):
        self._hookrelay.addhooks(hookspec)


class Plugin:
    """A plugin.

    This is a container for a plugin, which itself is usually a module
    or a class.  It provides additional information on plugins.

    Attributes:

    :obj: The actual plugin object.
    :index: The order in which this plugin was loaded.
    :name: The name of this plugin.

    """

    def __init__(self, obj, name, index):
        self.obj = obj
        self.name = name
        self.index = index


class HookImpl:
    """A plugin hook implementation

    This is a container for a plugin hook, which itself is a routine
    (function or method).  It provides additional information on the
    hook.

    Attributes:

    :routine: The actual function or method object.
    :plugin: The Plugin instance this hook belongs too.
    :name: The hook name.
    :first: Value from @hookimpl decorator, boolean.
    :last: Value from @hookimpl decorator, boolean.
    :before: Value from @hookimpl decorator, set.
    :after: Value from @hookimpl decorator, set.

    The constructor takes the routine as created by the @hookimpl
    decorator and converts the data correctly.

    """

    def __init__(self, routine, plugin):
        assert isinstance(plugin, Plugin)
        self.routine = routine
        self.plugin = plugin
        self.before = self._castset(routine.pm_hookimpl['before'])
        self.after = self._castset(routine.pm_hookimpl['after'])

    def _castset(self, obj):
        """Cast @hookimpl's "before" and "after" values to a set."""
        coll = set()
        if isinstance(obj, str):
            coll.add(obj)
        elif isinstance(obj, collections.Sequence):
            coll.update(obj)
        elif obj is None:
            pass
        else:
            raise TypeError('Invalid type for "before" argument '
                            'to @hookimpl for {!r} in {}'.format(
                                self.routine, self.routine.__module__))
        return coll

    def argnames(self):
        """Return the names of the hook arguments, in order"""
        try:
            return self._argnames
        except AttributeError:
            pass
        names = inspect.getargspec(self.routine).args
        if inspect.ismethod(self.routine):
            del names[0]
        self._argnames = names
        return names


class HookRelay:

    def __init__(self, hookspec=None):
        self.hooks = types.SimpleNamespace()
        if hookspec:
            self.addhooks(hookspec)

    def addhooks(self, hookspec):
        """Add new hooks from a hookspec.

        A hookspec can be either a module or a class which contains
        routines decorated using the @hookdef decorator.

        """
        added = False
        for name, routine in vars(hookspec).items():
            if hasattr(routine, 'pm_hookdef'):
                assert name == routine.pm_hookdef['name']
                if hasattr(self.hooks, name):
                    raise ValueError('Hook already exists for name: {}'
                                     .format(name))
                setattr(self.hooks, name, HookCaller(routine))
                added = True
        if not added:
            raise ValueError('No new hooks found in {!r}'.format(hookspec))

    def add_plugin(self, plugin):
        """Scan a plugin and register any hooks found.

        The plugin can be a module or a class and they will be scanned
        for routines marked as hooks using the @hookimpl decorator.
        Any hooks found will be registed and part of the next hook
        call.

        :param: plugin: A Plugin instance

        """
        for hookname, routine in vars(plugin.obj).items():
            if not hasattr(routine, 'pm_hookimpl'):
                continue
            assert hookname == routine.pm_hookimpl['name']
            try:
                hook = getattr(self.hooks, hookname)
            except AttributeError:
                raise ValueError('Found unknown hook: {}'.format(hookname))
            hookimpl = HookImpl(routine, plugin)
            hook.addimpl(hookimpl)

    def remove_plugin(self, plugin, name):
        pass


class HookCaller:
    """Callable to execute multiple routines as a hook

    This represent a hook callable which implements a multicall
    interface to hook implementations for a certain hook definition.

    Attributes:

    :name: The name of the hook.

    """
    # Private attributes:
    #
    # :_hookdef: The @hookdef decorated routine this hookcaller
    #    implements.
    #
    # :_hook_groups: Sorted tuple of lists of hooks.  Each list has
    #    hooks with identical before and after values.
    #
    # :_hooks: Tuple of all the hooks in call order.

    def __init__(self, hookdef):
        self._hookdef = hookdef
        self._hook_groups = tuple()
        self._hooks = tuple()
        argnames = inspect.getargspec(hookdef).args
        if inspect.ismethod(hookdef):
            del argnames[0]
        self._argnames = argnames
        self.name = hookdef.pm_hookdef['name']

    @property
    def firstresult(self):
        return self._hookdef.pm_hookdef['firstresult']

    def addimpl(self, hookimpl):
        """Add a hook implementation.

        :param hookimpl: A HookImpl instance.

        """
        # XXX This brute-forces the sorting problem.
        if hookimpl in self._hooks:
            raise ValueError('Hook implementation already registered: {!r}'
                             .format(hookimpl))
        for group in self._hook_groups:
            if (hookimpl.before == group[0].before and
                    hookimpl.after == group[0].after):
                group.append(hookimpl)
                group.sort(key=lambda h: h.plugin.index)
                self._hooks = self._flatten_groups(self._hook_groups)
                return
        groups = self._hook_groups + ([hookimpl],)
        for groups_order in itertools.permutations(groups):
            if self._check_order(groups_order):
                self._hook_groups = groups_order
                self._hooks = self._flatten_groups(self._hook_groups)
                break
        else:
            raise ValueError('Impossible to sort')

    @staticmethod
    def _flatten_groups(groups):
        return tuple(itertools.chain.from_iterable(groups))

    @staticmethod
    def _check_order(groups):
        """Return True if the order of the hook groups satisfies constraints.

        This checks "before" and "after" constraints of the hook
        groups' order.

        Return a boolean.

        """
        hooks = list(itertools.chain.from_iterable(groups))
        for i, hook in enumerate(hooks):
            before = set(h.plugin.name for h in hooks[:i])
            after = set(h.plugin.name for h in hooks[i+1:])
            if (hook.before.intersection(before) or
                    hook.after.intersection(after)):
                return False
        else:
            return True

    def removeimpl(self, hookimpl):
        pass

    def __call__(self, **kwargs):
        missing_args = set(self._argnames) - set(kwargs.keys())
        if missing_args:
            raise TypeError('{!r} call has missing args: {}'
                            .format(self, ' '.join(missing_args)))
        extra_args = set(kwargs.keys()) - set(self._argnames)
        if extra_args:
            raise TypeError('{!r} call has extra args: {}'
                            .format(self, ' '.join(extra_args)))
        results = []
        for hook in self._hooks:
            args = [kwargs[argname] for argname in hook.argnames()]
            res = hook.routine(*args)
            if res is not None:
                if self.firstresult:
                    return res
                results.append(res)
        if not self.firstresult:
            return results
        else:
            return None

    def __repr__(self):
        args = ', '.join(self._argnames)
        return '<HookCaller {}({})>'.format(self.name, args)
