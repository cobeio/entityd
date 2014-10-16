"""PluginManager Infrastructure."""


import inspect
import types
import weakref


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


def hookimpl(*func, before=None, after=None, first=False, last=False):
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

    :first: Mark that this hook must be called before any other
       plugins if possible.  It is still possible for other plugins to
       get their hooks called before this hook by using the ``before``
       keyword.  Only one plugin can use this option.

    :last: Mark that this hook must be called after any other plugins
       if possible.  It is still possible for other plugins to get
       their hooks called after this hook by using the ``after``
       keyword.  Only one plugin can use this option.

    If the hook implementation from a plugin conflicts with already
    loaded plugins an exception will be raised during loading of the
    plugin.

    """
    before_set = set()
    if isinstance(before, str):
        before_set.add(before)
    elif before:
        before_set.update(before)
    after_set = set()
    if isinstance(after, str):
        after_set.add(after)
    elif after:
        after_set.update(after)
    detail = {'before': before_set,
              'after': after_set,
              'first': first,
              'last': last}
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


class PluginManager:
    """The plugin manager."""

    def __init__(self, hookspec=None):
        self._plugins = {}
        self._hookrelay = HookRelay(hookspec)
        self.hooks = self._hookrelay.hooks

    def register(self, plugin, name=None):
        """Register a new plugin.

        If ``name`` is given it is used as the plugin name and will
        override it's ``__plugin_name__`` attribute.  Otherwise
        ``plugin.__plugin_name__`` will be used with a fallback to
        ``plugin.__name__``.  In any case the derrived name is
        assigned to ``plugin.__plugin_name__``.

        """
        if name:
            plugin.__plugin_name__ = name
        elif hasattr(plugin, '__plugin_name__'):
            name = plugin.__plugin_name__
        elif not hasattr(plugin, '__name__'):
            raise ValueError('No valid name found for plugin')
        else:
            plugin.__plugin_name__ = name = plugin.__name__
        if self.isregistered(plugin):
            raise ValueError('Plugin already registered: {}'.format(name))
        self._hookrelay.scan_plugin(plugin)

    def unregister(self, plugin):
        # plugin-or-name
        pass

    def isregistered(self, plugin):
        # plugin-or-name
        pass

    def getplugin(self, name):
        pass

    def addhooks(self, hookspec):
        self._hookrelay.addhooks(hookspec)


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
        for name, routine in vars(hookspec):
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

        """
        for hookname, hookimpl in vars(plugin):
            if not hasattr(hookimpl, 'pm_hookimpl'):
                continue
            assert hookname == hookimpl.pm_hookimpl['name']
            try:
                hook = getattr(self.hooks, hookname)
            except AttributeError:
                raise ValueError('Found unknown hook: {}'.format(hookname))
            hookimpl.pm_hookimpl['plugin'] = weakref.ref(plugin)
            hook.addimpl(hookimpl)

    def remove_plugin(self, plugin, name):
        pass


class HookCaller:
    """Callable to execute multiple routines as a hook

    This represent a hook callable which implements a multicall
    interface to hook implementations for a certain hook definition.

    """

    def __init__(self, hookdef):
        self._hookdef = hookdef
        self._routines = set()
        self._sorted = False
        self._call_list = []
        self._first = None
        self._last = None

    def addimpl(self, hookimpl):
        """Add a hook implementation. """
        if hookimpl in self._routines:
            raise ValueError('Hook implementation already registered: {!r}'
                             .format(hookimpl))
        assert hookimpl.pm_hookimpl['plugin']
        self._routines.add(hookimpl)
        if hookimpl.pm_hookimpl['first']:
            if self._first:
                raise ValueError(
                    'Only one hook implementation can be first for {name}, '
                    'original from {orig_plugin}, duplicate from {new_plugin}'
                    .format(
                        name=hookimpl.pm_hookimpl['name'],
                        orig_plugin=self._first.pm_hookimpl['plugin'],
                        new_plugin=hookimpl.pm_hookimpl['plugin'],
                    ))
            self._first = hookimpl
        if hookimpl.pm_hookimpl['last']:
            if self._last:
                raise ValueError(
                    'Only one hook implementation can be last for {name}, '
                    'original from {orig_plugin}, duplicate from {new_plugin}'
                    .format(
                        name=hookimpl.pm_hookimpl['name'],
                        orig_plugin=self._last.pm_hookimpl['plugin'],
                        new_plugin=hookimpl.pm_hookimpl['plugin'],
                    ))
        self._sort()

    def removeimpl(self, hookimpl):
        pass

    def __call__(self, **kwargs):
        if not self._sorted:
            self._sort()
        pass

    def _sort(self):
        pass
