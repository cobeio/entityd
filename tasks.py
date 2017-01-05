"""Some simple task automatisation.

At a bare minimum one should make sure the ``check`` command reports
no issues at all when submitting a pullrequest.
"""

import os
import pathlib
import sys
import xml.etree.ElementTree as etree

import act
import invoke
import zmq.auth


@invoke.task
def pylint(context):
    """Invoke pylint on modules and test code."""
    context.run('pylint entityd')
    context.run('cd tests; pylint **/*.py')
    context.run('pylint setup.py')
    context.run('pylint tasks.py')


@invoke.task
def pytest(context):
    """Run the entire test-suite."""
    context.run('py.test -q --cov-report=xml tests')
    tree = etree.parse('coverage.xml')  # Probably should use sax.
    root = tree.getroot()
    total = float(root.get('line-rate', 0))
    print('Test coverage: {:d}%'.format(int(total*100)))
    if total < 1:
        context.run('false')     # Crappy way of making the task fail


@invoke.task(pre=[pylint, pytest])
def check(context):  # pylint: disable=unused-argument
    """Perform all checks."""


@invoke.task(help={'dirpath': 'The base path to install keys under. Keys will'
                              ' be installed at `<dirpath>/entityd/keys` with'
                              ' any necessary directories being created. If'
                              ' not provided the default is '
                              '`<env>/etc/entityd/keys`.',
                   'force': 'Force overwriting of existing keys.',
                   'dry-run': 'Do a dry-run without making any keys.'})
def certificates(context, dirpath=None, force=False, dry_run=False):  # pylint: disable=unused-argument
    """Create certificates for ZMQ authentication."""
    dirpath = os.path.expanduser(dirpath) if dirpath else act.fsloc.sysconfdir
    dirpath = pathlib.Path(dirpath).absolute().joinpath('entityd', 'keys')
    for parent in [dirpath] + list(dirpath.parents):
        if parent.exists() and not parent.is_dir():
            print('Abort. Path exists and is not a directory: '
                  '{}'.format(parent))
            return
    if not dirpath.exists():  # pylint: disable=no-member
        print('Directory does not exist, creating: {}'.format(dirpath))
        if not dry_run:
            dirpath.mkdir(parents=True)  # pylint: disable=no-member
    for keyname in ['entityd.key', 'entityd.key_secret']:
        if dirpath.joinpath(keyname).exists() and not force:
            print('Abort. Key exists: {}'.format(dirpath.joinpath(keyname)))
            print('Use --force to overwrite any existing keys.')
            return
    if not dry_run:
        zmq.auth.create_certificates(str(dirpath), 'entityd')
    print('Created entityd.key and entityd.key_secret in {}'.format(dirpath))


# pylint: disable=invalid-name
namespace = invoke.Collection.from_module(sys.modules[__name__])
namespace.configure({
    'run': {
        'echo': True,
        'warn': True,
    },
})
