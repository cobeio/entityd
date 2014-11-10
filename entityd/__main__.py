"""Main application entrypoint.

This module is the main application entrypoint and ties creates a
plugin manager to drive the rest of the application's execution.

"""

import functools
import importlib
import logging
import sys

import entityd.hookspec
import entityd.pm
import entityd.version


#: These plugins are always loaded first and in order.
BUILTIN_PLUGIN_NAMES = ['entityd.' + n for n in
                        ['core', 'mesend', 'kvstore', 'hostme', 'processme']]


log = logging.getLogger('bootstrap')


def main(argv=None, plugins=None):
    """Application command line entrypoint.

    This creates a plugin manager, loads the default plugins and runs
    the ``entityd_main()`` hook.

    """
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    if plugins is None:
        plugins = BUILTIN_PLUGIN_NAMES
    pluginmanager = entityd.pm.PluginManager()
    if argv and '--trace' in argv:
        pluginmanager.tracer_cb = trace
    pluginmanager.addhooks(entityd.hookspec)
    register_cb = functools.partial(plugin_registered_cb, pluginmanager)
    pluginmanager.register_callback = register_cb
    for name in plugins:
        try:
            plugin = importlib.import_module(name)
        except Exception:       # pylint: disable=broad-except
            log.exception('Failed to import plugin: {}'.format(name))
            continue
        try:
            pluginmanager.register(plugin)
        except Exception:       # pylint: disable=broad-except
            log.exception('Failed to register plugin: {}'.format(name))
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
