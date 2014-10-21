import argparse
import logging

import entityd.pm
import entityd.version


@entityd.pm.hookimpl
def entityd_main(pluginmanager, argv):
    parser = argparse.ArgumentParser(
        prog='entityd',
        description='Entity Monitoring Agent',
    )
    pluginmanager.hooks.entityd_addoption(parser=parser)
    args = parser.parse_args(argv)
    config = Config(args)
    # XXX call some more hooks
    try:
        pluginmanager.hooks.entityd_mainloop(config=config)
    except Exception:
        raise                   # XXX return 1
    else:
        return 0


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
        help='log verbosity (0-100): 10=DEBUG, 20=INFO, 30=WARNING, 40=ERROR, 50=CRITICAL',
    )


@entityd.pm.hookimpl
def entityd_mainloop(config):
    print(config.args)


class Config:
    """The main configration instance."""

    def __init__(self, args):
        self.args = args        # XXX

    def addentity(self, name, plugin):
        """Register a plugin as providing a Monitored Entity.

        The given plugin needs to provide a number of hooks.  The
        plugin must already be registered.

        XXX Consider registering the plugin now if it isn't already
            but a few hooks might not be called and some ordering
            might not be possible.  Verifying all this is more work so
            keep it simple for now.

        """
