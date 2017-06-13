"""Core entityd application plugin.

This Module is the core entityd plugin which drives the entire
application.  This means it provides the ``entityd_main()`` hook which
drives the application by calling all other hooks.

The ``entityd_main()`` hook itself is called by the entityd.__main__
module which creates the plugin manager, registers the plugins and
then runs the application by calling this hook.

"""

import argparse
import contextlib
import threading
import time
import types

import act
import logbook
import logbook.compat
import pkg_resources

import entityd.pm


log = logbook.Logger(__name__)


@entityd.pm.hookimpl
def entityd_main(pluginmanager, argv):
    """Run entityd."""
    config = pluginmanager.hooks.entityd_cmdline_parse(
        pluginmanager=pluginmanager, argv=argv)
    log_handler = act.log.setup_logbook(config.args.log_level)
    with log_handler.applicationbound():
        logbook.compat.redirect_logging()
        with contextlib.ExitStack() as stack:
            stack.callback(pluginmanager.hooks.entityd_unconfigure)
            pluginmanager.hooks.entityd_configure(config=config)
            session = Session(pluginmanager, config)
            stack.callback(pluginmanager.hooks.entityd_sessionfinish,
                           session=session)
            pluginmanager.hooks.entityd_sessionstart(session=session)
            pluginmanager.hooks.entityd_mainloop(session=session)
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
        default=logbook.INFO,
        help='Log verbosity: debug, info, warning, error, critical; or 0-6',
        action=act.log.LogLevelAction,
    )
    parser.add_argument(
        '--trace',
        action='store_true',
        help='Trace the plugin manager actions',
    )
    parser.add_argument(
        '--period',
        default=60,
        type=lambda period: max(0, float(period)),
        help='How often to run periodic entity collection in seconds',
    )


@entityd.pm.hookimpl
def entityd_mainloop(session):
    """Run the daemon mainloop."""
    log.info('entityd started')
    try:
        session.run()
    except KeyboardInterrupt:
        pass


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

    When the session is running, entity collection is ran according to
    the configured periodicity. Once the collection process has finished,
    the session will suspend itself until its due to run again.

    .. note::
        If the entity collection process takes longer than the configured
        periodicity, the session may never suspend itself.

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
        time_next = time.monotonic()
        while not self._shutdown.is_set():
            time_now = time.monotonic()
            time_wait = max(0, time_next - time_now)
            if time_wait:
                log.debug(
                    'Suspending entity collection for {:.2f}s', time_wait)
            self._shutdown.wait(time_wait)
            if not self._shutdown.is_set():  # dont collect if shutting down
                time_start = time.monotonic()
                self.svc.monitor.collect_entities()
                time_next = time_start + self.config.args.period


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
