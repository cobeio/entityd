import functools
import importlib
import logging
import sys

import entityd.hookspec
import entityd.pm
import entityd.version


#: These plugins are always loaded first and in order, they are all
#: imported from the "entityd.{plugin}" namespace.
BUILTIN_PLUGIN_NAMES = ['core', 'mesend', 'hostme', 'storage']


log = logging.getLogger('bootstrap')


def main(argv=None):
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    pluginmanager = entityd.pm.PluginManager()
    if argv and '--trace' in argv:
        pluginmanager.tracer_cb = trace
    pluginmanager.addhooks(entityd.hookspec)
    register_cb = functools.partial(plugin_registered_cb, pluginmanager)
    pluginmanager.register_callback = register_cb
    for name in BUILTIN_PLUGIN_NAMES:
        modname = 'entityd.{}'.format(name)
        try:
            plugin = importlib.import_module(modname)
        except Exception:
            log.exception('Failed to import plugin: {}'.format(modname))
            continue
        try:
            pluginmanager.register(plugin)
        except Exception:
            log.exception('Failed to register plugin: {}'.format(modname))
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
    except Exception:
        log.exception('Failed to call entityd_plugin_registered hook:')


def trace(msg):
    print('TRACE: {}'.format(msg))


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
