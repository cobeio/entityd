import argparse
import logging
import sys

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


if __name__ == '__main__':
    main()
