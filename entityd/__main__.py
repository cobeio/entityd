import argparse
import logging
import sys

import entityd.mesend
import entityd.version


def parse_cmdline(argv=None):
    parser = argparse.ArgumentParser(
        prog='entityd',
        description='Entity Monitoring Agent',
    )
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
    parser.add_argument(
        '--dest',                         # XXX Choose better name
        default='tcp://127.0.0.1:25010',  # XXX Should not have a default
        type=str,
        help='ZeroMQ address of modeld destination',
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_cmdline(argv)
    logging.basicConfig(stream=sys.stdout, level=args.log_level)
    log = logging.getLogger()
    log.info(args)
    sender = entityd.mesend.MonitoredEntitySender(args.dest)
    sender.send(b'hi there')


class Config:
    """The main configration instance."""

    def addentity(self, name, plugin):
        """Register a plugin as providing a Monitored Entity.

        The given plugin needs to provide a number of hooks.  The
        plugin must already be registered.

        XXX Consider registering the plugin now if it isn't already
            but a few hooks might not be called and some ordering
            might not be possible.  Verifying all this is more work so
            keep it simple for now.

        """


if __name__ == '__main__':
    main()
