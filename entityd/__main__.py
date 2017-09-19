"""Main application entrypoint.

This module is the main application entrypoint and ties creates a
plugin manager to drive the rest of the application's execution.
"""

import fnmatch
import functools
import importlib
import sys

import logbook

import entityd.hookspec
import entityd.pm


#: These plugins are always loaded first and in order.
BUILTIN_PLUGIN_NAMES = ['entityd.' + n for n in
                        ['core',
                         'mesend:MonitoredEntitySender',
                         'kvstore',
                         'monitor:Monitor',
                         'hostme:HostEntity',
                         'processme:ProcessEntity',
                         'endpointme:EndpointEntity',
                         'apacheme:ApacheEntity',
                         'mysqlme:MySQLEntity',
                         'postgresme:PostgreSQLEntity',
                         'fileme:FileEntity',
                         'kubernetes',
                         'kubernetes.node:NodeEntity',
                         'kubernetes.cluster:ClusterEntity',
                         'kubernetes.deployment:DeploymentEntity',
                         'kubernetes.service:ServiceEntity',
                         'kubernetes.replicaset:ReplicaSetEntity',
                         'kubernetes.replicationcontroller:ReplicationControllerEntity', # pylint: disable=line-too-long
                         'kubernetes.daemonset:DaemonSetEntity',
                         'kubernetes.kubernetes',
                         'kubernetes.group:NamespaceGroup',
                         'declentity:DeclarativeEntity',
                         'docker.container:DockerContainer',
                         'docker.daemon:DockerDaemon',
                         'docker.container_group:DockerContainerGroup',]]


log = logbook.Logger('entityd.bootstrap')


def main(argv=None, plugins=None):
    """Application command line entrypoint.

    This creates a plugin manager, loads the default plugins and runs
    the ``entityd_main()`` hook.
    """
    if argv is None:
        argv = sys.argv[1:]
    if plugins is None:
        plugins = BUILTIN_PLUGIN_NAMES
    plugins = _filter_disabled_plugins(argv or [], plugins)
    pluginmanager = entityd.pm.PluginManager()
    if argv and '--trace' in argv:
        pluginmanager.tracer_cb = trace
    pluginmanager.addhooks(entityd.hookspec)
    register_cb = functools.partial(plugin_registered_cb, pluginmanager)
    pluginmanager.register_callback = register_cb
    for name in plugins:
        classname = None
        if ':' in name:
            modname, classname = name.split(':')
        else:
            modname = name
        try:
            mod = importlib.import_module(modname)
        except Exception:       # pylint: disable=broad-except
            print('Failed to import plugin module: {}'.format(name))
            continue
        if classname:
            try:
                cls = getattr(mod, classname)
            except AttributeError:
                print('Failed to get plugin class: {}'.format(name))
                continue
            try:
                plugin = cls()
            except Exception:  # pylint: disable=broad-except
                print('Failed to instantiate class: {}'.format(name))
                continue
            name = '.'.join([plugin.__module__, cls.__name__])
        else:
            plugin = mod
            name = mod.__name__
        try:
            pluginmanager.register(plugin, name=name)
        except Exception as error:       # pylint: disable=broad-except
            print('Failed to register plugin: {} {}'.format(name, error))
    return pluginmanager.hooks.entityd_main(
        pluginmanager=pluginmanager,
        argv=argv,
    )


def plugin_registered_cb(pluginmanager, plugin):
    """Callback used by the PluginManager when a new plugin is registered.

    This simply calls the "entityd_plugin_registered" hook.

    """
    try:
        pluginmanager.hooks.entityd_plugin_registered(
            pluginmanager=pluginmanager,
            name=plugin.name,
        )
    except Exception:           # pylint: disable=broad-except
        log.exception('Failed to call entityd_plugin_registered hook:')


def trace(msg):
    """Simplistic trace function.

    This prints trace messages directly to stdout.
    """
    print('TRACE: {}'.format(msg))


def _parse_disabled_plugins(arguments):
    """Parse list of plugins to disable from command line arguments.

    This parses a list of command line arguments looking for --disable
    switches. Any arguments that follow are treated as plugin to disable,
    until a new switch is encountered.

    Alternatively, the --disable= prefixed form may be used which only
    allows for a single plugin to be specified.

    If the disable argument refers to an entire module -- e.g. it doesn't
    contain a colon -- then it implicitly ignores everything from that
    module as well. As in, `fileme` is treated as `fileme:*`.

    :param arguments: The raw command line arguments to parse.
    :type arguments: list of str

    :returns: An iterator of plugins to disable.
    """
    accumulate = False
    for argument in (argument.strip() for argument in arguments):
        disabled = None
        if argument.startswith('-'):
            accumulate = False
            if argument == '--disable':
                accumulate = True
            elif argument.startswith('--disable='):
                disabled = argument[len('--disable='):]
        elif accumulate:
            disabled = argument
        if disabled is not None:
            if ':' not in disabled:
                yield disabled + ':*'
            yield disabled


def _filter_disabled_plugins(arguments, plugins):
    """Filter out disabled plugins.

    This filters plugins from the given list of plugins based on given
    --disable arguments.

    The --disable arguments are treated as globs against the plugin names.
    Any plugin names that match any of the globs are not included in the
    list of returned plugins.

    Each disable pattern is prefixed with `entityd.`.

    :param arguments: The raw command line arguments to parse.
    :type arguments: list of str
    :param plugins: List of all plugin names.
    :type plugins: list of str

    :returns: A filtered list of plugin names.
    """
    plugins_disabled = ['entityd.' + disabled
                        for disabled in _parse_disabled_plugins(arguments)]
    plugins_enabled = []
    for plugin in plugins:
        if not any(fnmatch.fnmatchcase(
                plugin, disabled) for disabled in plugins_disabled):
            plugins_enabled.append(plugin)
    return plugins_enabled


if __name__ == '__main__':      # pragma: no cover
    sys.exit(main(sys.argv[1:]))
