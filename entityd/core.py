import argparse
import logging
import time

import entityd.pm
import entityd.version


@entityd.pm.hookimpl
def entityd_main(pluginmanager, argv):
    config = pluginmanager.hooks.entityd_cmdline_parse(
        pluginmanager=pluginmanager, argv=argv)
    pluginmanager.hooks.entityd_configure(config=config)
    session = MonitorSession(pluginmanager, config)
    pluginmanager.hooks.entityd_sessionstart(session=session)
    pluginmanager.hooks.entityd_mainloop(session=session)
    pluginmanager.hooks.entityd_sessionfinish(session=session)
    pluginmanager.hooks.entityd_unconfigure(config=config)
    return 0


@entityd.pm.hookimpl
def entityd_cmdline_parse(pluginmanager, argv):
    parser = argparse.ArgumentParser(
        prog='entityd',
        description='Entity Monitoring Agent',
    )
    pluginmanager.hooks.entityd_addoption(parser=parser)
    args, unknown = parser.parse_known_args(argv)
    if unknown:
        logging.warning("Ignoring unknown arguments {}".format(unknown))
    return Config(pluginmanager, args)


@entityd.pm.hookimpl
def entityd_addoption(parser):
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


class MonitorSession:
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

    def run(self):
        """Run the monitoring session.

        This will block until .shutdown() is called or SIGTERM is
        received (aka KeyboardInterrupt is raised).

        """
        while True:
            t = time.time()
            self.collect_entities()
            print("Took {}".format(time.time() - t))
            time.sleep(60)

    def shutdown(self):
        pass

    def collect_entities(self):
        """Collect and send all Monitored Entities."""
        for metype in self.config.entities:
            result, = self.pluginmanager.hooks.entityd_find_entity(name=metype,
                                                                   attrs=None)
            for entity in result:
                self.pluginmanager.hooks.entityd_send_entity(session=self,
                                                             entity=entity)
