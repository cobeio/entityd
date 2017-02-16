"""Main application entrypoint.

This module is the main application entrypoint and ties creates a
plugin manager to drive the rest of the application's execution.

"""

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
                         'kubernetes.node:NodeEntity',
                         'kubernetes.cluster:ClusterEntity',
                         'kubernetes.deployment:DeploymentEntity',
                         'kubernetes.service:ServiceEntity',
                         'kubernetes.replicaset:ReplicaSetEntity',
                         'kubernetes.'
                         'replicationcontroller:ReplicationControllerEntity',
                         'kubernetes.daemonset:DaemonSetEntity',
                         'kubernetes.kubernetes',
                         'declentity:DeclarativeEntity']]


log = logbook.Logger('entityd.bootstrap')


def main(argv=None, plugins=None):
    """Application command line entrypoint.

    This creates a plugin manager, loads the default plugins and runs
    the ``entityd_main()`` hook.

    """
    if plugins is None:
        plugins = BUILTIN_PLUGIN_NAMES
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
            log.exception('Failed to import plugin module: {}', name)
            continue
        if classname:
            try:
                cls = getattr(mod, classname)
            except AttributeError:
                log.exception('Failed to get plugin class: {}', name)
                continue
            try:
                plugin = cls()
            except Exception:  # pylint: disable=broad-except
                log.exception('Failed to instantiate class: {}', name)
                continue
            name = '.'.join([plugin.__module__, cls.__name__])
        else:
            plugin = mod
            name = mod.__name__
        try:
            pluginmanager.register(plugin, name=name)
        except Exception:       # pylint: disable=broad-except
            log.exception('Failed to register plugin: {}', name)
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


if __name__ == '__main__':      # pragma: no cover
    sys.exit(main(sys.argv[1:]))
