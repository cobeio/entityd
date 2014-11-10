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
import types

import entityd.pm
import entityd.version


@entityd.pm.hookimpl
def entityd_main(pluginmanager, argv):
    """Run entityd."""
    config = pluginmanager.hooks.entityd_cmdline_parse(
        pluginmanager=pluginmanager, argv=argv)
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
    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s {}'.format(entityd.version.__version__),
    )
    parser.add_argument(
        '-l', '--log-level',
        metavar='N',
        default=logging.INFO,
        type=int,
        help=('log verbosity (0-100): 10=DEBUG, '
              '20=INFO, 30=WARNING, 40=ERROR, 50=CRITICAL'),
    )
    parser.add_argument(
        '--trace',
        action='store_true',
        help='Trace the plugin manager actions',
    )


@entityd.pm.hookimpl
def entityd_mainloop(session):
    """Run the daemon mainloop."""
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
            self.collect_entities()
            self._shutdown.wait(60)

    def shutdown(self):
        """Signal the session to shutdown.

        This does not wait until the shutdown has finished.

        """
        self._shutdown.set()

    def collect_entities(self):
        """Collect and send all Monitored Entities."""
        for metype in self.config.entities:
            result, = self.pluginmanager.hooks.entityd_find_entity(name=metype,
                                                                   attrs=None)
            for entity in result:
                self.pluginmanager.hooks.entityd_send_entity(session=self,
                                                             entity=entity)

    def addservice(self, name, routine):
        """Register a service provided by a plugin.

        Services will be provided by the Session instance as
        ``Session.svc.{name}`` callable.  Their main property is that
        only one plugin can provide a given service, they are simply
        callables and need to have unique names.

        :param name: The name of the service.

        :param routine: A callable which provides the service.

        :raises KeyError: If a service is already registerd for the
           given name.

        """
        if hasattr(self.svc, name):
            raise KeyError('Service already registerd: {}'.format(name))
        else:
            setattr(self.svc, name, routine)
