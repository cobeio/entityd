"""Core entityd application plugin.

This Module is the core entityd plugin which drives the entire
application.  This means it provides the ``entityd_main()`` hook which
drives the application by calling all other hooks.

The ``entityd_main()`` hook itself is called by the entityd.__main__
module which creates the plugin manager, registers the plugins and
then runs the application by calling this hook.

"""

import argparse
import logging
import threading
import sys
import time
import types

import pkg_resources
import entityd.pm


log = logging.getLogger(__name__)


@entityd.pm.hookimpl
def entityd_main(pluginmanager, argv):
    """Run entityd."""
    config = pluginmanager.hooks.entityd_cmdline_parse(
        pluginmanager=pluginmanager, argv=argv)
    setup_logging(config)
    pluginmanager.hooks.entityd_configure(config=config)
    session = Session(pluginmanager, config)
    pluginmanager.hooks.entityd_sessionstart(session=session)
    pluginmanager.hooks.entityd_mainloop(session=session)
    pluginmanager.hooks.entityd_sessionfinish(session=session)
    pluginmanager.hooks.entityd_unconfigure(config=config)
    return 0


@entityd.pm.hookimpl
def entityd_cmdline_parse(pluginmanager, argv):
    """Parse the command line arguments.

    Returns an instantiated Config object.
    """
    parser = argparse.ArgumentParser(
        prog='entityd',
        description='Entity Monitoring Agent',
    )
    pluginmanager.hooks.entityd_addoption(parser=parser)
    args = parser.parse_args(argv)
    return Config(pluginmanager, args)


@entityd.pm.hookimpl
def entityd_addoption(parser):
    """Add command line options to the argparse parser."""
    version = pkg_resources.require('entityd')[0].version  # pylint: disable=not-callable
    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s {}'.format(version),
    )
    parser.add_argument(
        '-l', '--log-level',
        metavar='LEVEL',
        default=logging.INFO,
        help=('log verbosity; debug, info, '
              'warning, error or critical; or 0-100'),
        action=LogLevelAction,
    )
    parser.add_argument(
        '--trace',
        action='store_true',
        help='Trace the plugin manager actions',
    )


@entityd.pm.hookimpl
def entityd_mainloop(session):
    """Run the daemon mainloop."""
    log.info('entityd started')
    try:
        session.run()
    except KeyboardInterrupt:
        pass


def setup_logging(config):
    """Setup the global logging infrastructure.

    :param config: The Config instance.

    :return: The root logger instance.
    """
    log_format = '{asctime} {levelname:8} {name:14} {message}'
    root_logger = logging.getLogger()
    root_handler = logging.StreamHandler(stream=sys.stdout)
    root_formatter = logging.Formatter(fmt=log_format, style='{')
    root_formatter.converter = time.gmtime
    root_handler.setFormatter(root_formatter)
    root_logger.setLevel(config.args.log_level)
    root_logger.addHandler(root_handler)
    return root_logger


class LogLevelAction(argparse.Action):
    """Custom parser action to validate loglevel as either int or string."""

    def __init__(self, option_strings, dest, nargs=None, **kwargs):
        super().__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        try:
            values = int(values)
        except ValueError:
            try:
                values = getattr(logging, values.upper())
            except AttributeError:
                raise argparse.ArgumentError(
                    self, "Invalid log level: {}".format(values))
        setattr(namespace, self.dest, values)


class Config:
    """The main configuration instance.

    Attributes:

    :pluginmanager: The PluginManager instance.
    :args: The argparse Namespace from parsing the command line.
    :entities: Dict of Monitored Entity names mapped to the plugin
       providing them.

    """

    def __init__(self, pluginmanager, args):
        self.args = args
        self.pluginmanager = pluginmanager
        self.entities = dict()

    def addentity(self, name, plugin):
        """Register a plugin as providing a Monitored Entity.

        The given plugin needs to provide a number of hooks.  The
        plugin must already be registered.  It is usual to call this
        in the entityd_configure() hook of the plugin providing the
        entity.

        :param name: The name of the Monitored Entity.

        :param plugin: The plugin providing the Monitored Entity.
           This can be a plugin name, the plugin object or the
           entityd.pm.Plugin instance.

        :raises KeyError: If the Monitored Entity already exists a
           KeyError is raised.

        XXX Consider registering the plugin now if it isn't already
            but a few hooks might not be called and some ordering
            might not be possible.  Verifying all this is more work so
            keep it simple for now.

        """
        if name in self.entities:
            raise KeyError(
                'Monitored Entity already registered: {}'.format(name))
        plugin = self.pluginmanager.getplugin(plugin)
        self.entities[name] = plugin

    def removeentity(self, name, plugin):
        """Unregister a plugin as providing a Monitored Entity.

        :param name: The name of the Monitored Entity.

        :param plugin: The plugin providing the Monitored Entity.
           This can be a plugin name, the plugin object or the
           entityd.pm.Plugin instance.

        :raises KeyError: If the Monitored Entity does not exist a KeyError is
            raised. If the Monitored Entity is registered with a different
            plugin than given a KeyError is raised.
        """
        if name not in self.entities:
            raise KeyError(
                'Monitored Entity not registered: {}'.format(name))
        plugin = self.pluginmanager.getplugin(plugin)
        if self.entities[name] != plugin:
            raise KeyError(
                'Unregistering plugin {} does not match registered plugin {}'
                .format(plugin, self.entities[name]))
        del self.entities[name]

class Session:
    """A monitoring session.

    Attributes:

    :config: The Config instance.
    :pluginmanager: The PluginManager instance.

    XXX This is currently way to simplistic, monitoring in this way
        would result in resources spikes etc.  It may also be that the
        actual monitoring activity should be moved to it's own plugin.

    """

    def __init__(self, pluginmanager, config):
        self.config = config
        self.pluginmanager = pluginmanager
        self._shutdown = threading.Event()
        self.svc = types.SimpleNamespace()

    def run(self):
        """Run the monitoring session.

        This will block until .shutdown() is called or SIGTERM is
        received (aka KeyboardInterrupt is raised).

        :raises KeyBoardInterrupt: When SIGTERM is received the
           KeyBoardInterrupt is not caught and propagated up to the
           caller.

        """
        while not self._shutdown.is_set():
            self.svc.monitor.collect_entities()
            self._shutdown.wait(60)

    def shutdown(self):
        """Signal the session to shutdown.

        This does not wait until the shutdown has finished.

        """
        self._shutdown.set()

    def addservice(self, name, obj):
        """Register a service provided by a plugin.

        Services will be provided by the Session instance as then
        ``Session.svc.{name}`` object.  Their main property is that
        only one plugin can provide a given service, they are simply
        objects and need to have unique names.  Normally a service
        would be a direct callable or an instance with callable
        methods.

        :param name: The name of the service.

        :param obj: An object which provides the service.

        :raises KeyError: If a service is already registered for the
           given name.

        """
        if hasattr(self.svc, name):
            raise KeyError('Service already registered: {}'.format(name))
        else:
            setattr(self.svc, name, obj)
