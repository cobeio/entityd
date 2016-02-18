"""PluginManager Infrastructure."""

import collections
import inspect
import itertools
import types


def hookdef(*func, firstresult=False):
    """Declare decorated function as a hook definition.

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
        def _hookdef(func):     # pylint: disable=missing-docstring
            detail.update(name=func.__name__)
            func.pm_hookdef = detail
            return func
        return _hookdef


def hookimpl(*func, before=None, after=None):
    """Declare a function or method as a hook implementation.

    If the func argument is given then it must be a callable and this
    function is a decorator which marks the callable as a hook
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
        def _hookimpl(func):    # pylint: disable=missing-docstring
            detail.update(name=func.__name__)
            func.pm_hookimpl = detail
            return func
        return _hookimpl


class PluginManager:
    """The plugin manager.

    Attributes:

    :hooks: This is the HookRelay.hooks namespace used to call hooks.
       It is a namespace object with a HookCaller instance for each
       registered hook which allows calling of a hook using:
       ``pluginmanager.hooks.my_hook(param=val)``.

    """
    # pylint: disable=too-many-instance-attributes

    def __init__(self, hookspec=None):
        """Create a new PluginManager instance.

        :param hookspec: If given this is passed to the HookRelay and
           used to extract hook definitions.  Calling .addhooks()
           achieves the same.

        """
        self._plugins = {}
        self._plugin_count = itertools.count()
        self._register_cb = None
        self._tracer_cb = None
        self._hookrelay = HookRelay(self._trace, hookspec)
        self.hooks = self._hookrelay.hooks

    def register(self, obj, name=None):
        """Register a new plugin.

        :param plugin: The object (module or class) which contains the
           hook routines.

        :param name: The name to give to the function.  If not given
           this will try to use ``plugin.__name__`` if it exists,
           otherwise a ValueError is raised.

        :returns: The created Plugin instance is returned.

        """
        index = next(self._plugin_count)
        if not name:
            try:
                name = obj.__name__
            except AttributeError:
                raise ValueError('Missing plugin name')
        plugin = Plugin(obj, name, index)
        self._trace('Registering plugin: {}'.format(plugin))
        if self.isregistered(plugin):
            raise ValueError('Plugin already registered: {}'.format(name))
        self._hookrelay.addplugin(plugin)
        self._plugins[name] = plugin
        if self._register_cb:
            self._register_cb(plugin)
        return plugin

    def unregister(self, plugin):
        """Remove the given plugin.

        :param plugin: Can be either a Plugin instance, plugin name or
           an object.

        """
        plugin = self.getplugin(plugin)
        self._plugins.pop(plugin.name)
        self._hookrelay.removeplugin(plugin)

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
        """Set the plugin registration callback."""
        self._register_cb = cb

    @property
    def tracer_cb(self):
        """Callback to trace the plugin manager and hook invocations.

        Assign a callable to this which will be called with a log
        message every time the plugin manager does something notable,
        like e.g. when a hook is invoked etc.

        """
        return self._tracer_cb

    @tracer_cb.setter
    def tracer_cb(self, cb):
        """Set the tracer callback."""
        self._tracer_cb = cb

    def _trace(self, msg):
        """Log a message about what the plugin manager is doing."""
        if self._tracer_cb:
            self._tracer_cb(msg)

    def isregistered(self, plugin):
        """Return True if the matching plugin is registered.

        :param plugin: Can be either a Plugin instance, plugin name or
           an object.

        Return False if no such plugin is registered.

        """
        try:
            self.getplugin(plugin)
        except LookupError:
            return False
        else:
            return True

    def getplugin(self, plugin):
        """Return the matching Plugin instance.

        :param plugin: Can be either a Plugin instance, plugin name or
           an object.

        Raises LookupError if the plugin is not registered.

        """
        err = LookupError('Plugin not registered: {}'.format(plugin))
        if isinstance(plugin, Plugin):
            if plugin.name not in self._plugins:
                raise err
            return plugin
        elif isinstance(plugin, str):
            if plugin not in self._plugins:
                raise err
            return self._plugins[plugin]
        else:
            for box in self._plugins.values():
                if plugin is box.obj:
                    return box
            raise err

    def addhooks(self, hookspec):
        """Add new hook definitions.

        :param hookspec: A module or class which is scanned for hook
           definitions.  A hook is discovered only as a direct member
           of the given object and must be marked up with the @hookdef
           decorator.

        """
        self._hookrelay.addhooks(hookspec)


class Plugin:
    """A plugin.

    This is a container for a plugin, which itself is usually a module
    or a class.  It provides additional information on plugins.

    Attributes:

    :obj: The actual plugin object.
    :index: The order in which this plugin was loaded.
    :name: The name of this plugin.
    :hooks: A list of HookImpl instances this plugin provides.

    """

    def __init__(self, obj, name, index):
        """Create a new Plugin container.

        :param obj: The object providing the plugin, a module or instance.

        :param name: The name to refer to the plugin.

        :param index: The relative index of this plugin, this is used
           to order called hooks from different plugins if no other
           ordering is implied.

        """
        self.obj = obj
        self.name = name
        self.index = index
        self.hooks = []
        for hookname, routine in inspect.getmembers(obj):
            if not hasattr(routine, 'pm_hookimpl'):
                continue
            assert hookname == routine.pm_hookimpl['name']
            self.hooks.append(HookImpl(routine, self))

    def __repr__(self):
        return '<Plugin {}>'.format(self.name)


class HookImpl:
    """A plugin hook implementation.

    This is a container for a plugin hook, which itself is a routine
    (function or method).  It provides additional information on the
    hook.

    Attributes:

    :routine: The actual function or method object.
    :plugin: The Plugin instance this hook belongs too.
    :name: The hook name.
    :before: Value from @hookimpl decorator, set.
    :after: Value from @hookimpl decorator, set.

    The constructor takes the routine as created by the @hookimpl
    decorator and converts the data correctly.

    """

    def __init__(self, routine, plugin):
        """Create a new container for a hook implementation.

        :param routine: The actual function or method implementing the
           hook.

        :param plugin: The Plugin instance providing this hook.

        """
        assert isinstance(plugin, Plugin)
        self.routine = routine
        self.plugin = plugin
        self.name = routine.pm_hookimpl['name']
        self.before = self._castset(routine.pm_hookimpl['before'])
        self.after = self._castset(routine.pm_hookimpl['after'])
        self._argnames = None

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
        """Return the names of the hook arguments, in order."""
        if self._argnames is None:
            self._argnames = inspect.signature(self.routine).parameters.keys()
        return self._argnames

    def __repr__(self):
        return '<HookImpl {}:{}>'.format(self.plugin.name, self.name)


class HookRelay:
    """Hook dispatcher.

    This is where hookspecs and plugins are registered and tied
    together.  For each hook definition found in the hookspecs a
    HookCaller is attached to the .hooks attribute.  Any added plugin
    which implements the hook will then get their hook implementation
    added to this hook caller.  And thus calls from .hooks.my_hook()
    are relayed to all the plugins implementing them.

    Attributes:

    :hooks: A simple namespace object which will get HookCaller
       instances assigned as attributes for each hook definition
       registered by hookspecs.

    """

    def __init__(self, trace, hookspec=None):
        """Create a new hookrelay.

        :param trace: Must be a callable which will be used to relay
           simple messages about hooks being registered and called.
           Use something like ``lambda msg: None`` to not trace
           anything.

        :param hookspec: A hookspec module, this is a module which has
           functions decoratored using @hookdef to mark them as hook
           definitions.  For each hook definition found a HookCaller
           will be created and added to ``.hooks``.  See
           ``.addhooks()`` which does the same.

        """
        self.hooks = types.SimpleNamespace()
        self._trace = trace
        if hookspec:
            self.addhooks(hookspec)

    def addhooks(self, hookspec):
        """Add new hooks from a hookspec.

        A hookspec can be either a module or a class which contains
        routines decorated using the @hookdef decorator.

        """
        added = False
        for name, routine in inspect.getmembers(hookspec):
            if hasattr(routine, 'pm_hookdef'):
                assert name == routine.pm_hookdef['name']
                if hasattr(self.hooks, name):
                    raise ValueError('Hook already exists for name: {}'
                                     .format(name))
                setattr(self.hooks, name, HookCaller(routine, self._trace))
                added = True
                self._trace('Added hookdef {} from {}'.format(name, hookspec))
        if not added:
            raise ValueError('No new hooks found in {!r}'.format(hookspec))

    def addplugin(self, plugin):
        """Scan a plugin and register any hooks found.

        The plugin can be a module or a class and they will be scanned
        for routines marked as hooks using the @hookimpl decorator.
        Any hooks found will be registered and part of the next hook
        call.

        :param: plugin: A Plugin instance

        """
        for impl in plugin.hooks:
            try:
                hook = getattr(self.hooks, impl.name)
            except AttributeError:
                raise ValueError('Found unknown hook in {}: {}'
                                 .format(plugin, impl.name))
            hook.addimpl(impl)

    def removeplugin(self, plugin):
        """Remove hook implementations provided by the plugin.

        :param plugin: A Plugin instance.

        """
        for impl in plugin.hooks:
            hook = getattr(self.hooks, impl.name)
            hook.removeimpl(impl)


class HookCaller:
    """Callable to execute multiple routines as a hook.

    This represent a hook callable which implements a multicall
    interface to hook implementations for a certain hook definition.

    :param hookdef_func: the hook definition function.

    :param trace: Function to handle simple trace messages.

    Attributes:

    :name: The name of the hook.

    :firstresult: Whether the result from the first hook should be
        used.  Normally all hook implementations will be called and
        the caller will receive a list of all results.  When this is
        True however only the first hook returning a value, i.e. not
        returning None, will be returned.

    """

    # Private attributes:
    #
    # :_hookdef: The @hookdef decorated routine this hookcaller
    #    implements.
    #
    # :_hooks: List of all the hooks in call order.
    #
    # :_argnames: List of the argument names the hook accepts.

    def __init__(self, hookdef_func, trace):
        self._hookdef = hookdef_func
        self._trace = trace
        self._hooks = []
        self._argnames = inspect.signature(hookdef_func).parameters.keys()
        self.name = hookdef_func.pm_hookdef['name']
        self.firstresult = self._hookdef.pm_hookdef['firstresult']

    def addimpl(self, impl):
        """Add a hook implementation.

        :param hookimpl: A HookImpl instance.

        """
        if impl in self._hooks:
            raise ValueError('Hook implementation already registered: {!r}'
                             .format(impl))
        unknown_args = set(impl.argnames()) - set(self._argnames)
        if unknown_args:
            raise TypeError('Hook {} accepts unknown arguments: {}'
                            .format(impl, ', '.join(unknown_args)))
        all_hooks = list(self._hooks) + [impl]
        self._hooks = self.sort_hooks(all_hooks)
        self._trace('Added hook: {}'.format(impl))

    def sort_hooks(self, hooks):
        """Sort hooks by moving them according to their constraints.

        We give each hook a value. The most significant digit is determined by
        the before and after hook constraints. The less significant digit is
        the plugin index and gives a unique solution.

        Before sorting them, we create equivalent 'after' constraints from
        any 'before' constraints to simplify the code.

        The original hooks are not modified.

        :param hooks: List of hooks to sort.
        :return: List of hooks in sorted order.
        """
        HookValue = collections.namedtuple('HookValue', 'hook value after')  # pylint: disable=invalid-name
        hook_values = [HookValue(hook,
                                 [hook.plugin.index, hook.plugin.index],
                                 set(hook.after)) for hook in hooks]

        # Replace all 'before' entries with the equivalent 'after' constraint.
        for hook, value, after in hook_values:
            for other in hooks:
                if hook.plugin.name in other.before:
                    after.add(other.plugin.name)

        for _ in range(len(hooks) ** 2):
            sorted_hooks = [hv.hook for hv in
                            sorted(hook_values, key=lambda h: h.value)]
            ordered = self.correctly_ordered(sorted_hooks)
            if ordered:
                return sorted_hooks
            for hook, value, after in hook_values:
                if after:
                    max_value = max(h.value for h in
                                    hook_values if h.hook.plugin.name in after)
                    value[0] = max_value[0] + 1
        raise ValueError('Impossible to sort.')

    @staticmethod
    def correctly_ordered(hooks):
        """Check if `hooks` is correctly ordered according to before and after
        constraints.

        :param hooks: List of hooks in order to check.
        :return: True if the hooks are correctly ordered, else False.
        """
        for i, current in enumerate(hooks):
            before = hooks[:i] if i else []
            after = hooks[i + 1:]
            for other in current.after:
                if other == current.plugin.name:
                    return False
                if other in [h.plugin.name for h in after]:
                    return False
            for other in current.before:
                if other == current.plugin.name:
                    return False
                if other in [h.plugin.name for h in before]:
                    return False
        return True

    def removeimpl(self, impl):
        """Remove a hook implementation.

        :param impl: HookImpl instance to remove.

        :raises ValueError: When impl is not present.
        """
        self._hooks.remove(impl)

    def __call__(self, **kwargs):
        extra_args = set(kwargs.keys()) - set(self._argnames)
        if extra_args:
            raise TypeError('{!r} call has extra args: {}'
                            .format(self, ' '.join(extra_args)))
        results = []
        for hook in self._hooks:
            args = [kwargs.get(argname) for argname in hook.argnames()]
            self._trace('Calling hook: {}'.format(hook))
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
