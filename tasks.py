"""Some simple task automatisation.

At a bare minimum one should make sure the ``check`` command reports
no issues at all when submitting a pullrequest.

"""

import os
import pathlib
import xml.etree.ElementTree as etree

import act
import invoke
import zmq.auth


@invoke.task
def pylint():
    """Invoke pylint on modules and test code."""
    print(' Invoking pylint '.center(80, '+'))
    invoke.run('pylint entityd')
    invoke.run('cd tests; pylint *.py')
    invoke.run('pylint setup.py')
    invoke.run('pylint tasks.py')


@invoke.task
def pytest():
    """Run the entire test-suite."""
    print(' Invoking py.test '.center(80, '+'))
    invoke.run('py.test -q --cov-report=xml tests')
    tree = etree.parse('coverage.xml')  # Probably should use sax.
    root = tree.getroot()
    total = float(root.get('line-rate', 0))
    print('Test coverage: {:d}%'.format(int(total*100)))
    if total < 1:
        invoke.run('false')     # Crappy way of making the task fail


@invoke.task
def check():
    """Perform all checks."""
    try:
        pylint()
    except invoke.exceptions.Failure as err:
        error = err
    else:
        error = None
    pytest()
    if error:
        raise error


@invoke.task(help={'dirpath': 'The base path to install keys under. Keys will'
                              ' be installed at `<dirpath>/entityd/keys` with'
                              ' any necessary directories being created. If'
                              ' not provided the default is '
                              '`<env>/etc/entityd/keys`.',
                   'force': 'Force overwriting of existing keys.',
                   'dry-run': 'Do a dry-run without making any keys.'})
def certificates(dirpath=None, force=False, dry_run=False):
    """Create certificates for ZMQ authentication."""
    dirpath = os.path.expanduser(dirpath) if dirpath else act.fsloc.sysconfdir
    dirpath = pathlib.Path(dirpath).absolute().joinpath('entityd', 'keys')
    for parent in [dirpath] + list(dirpath.parents):
        if parent.exists() and not parent.is_dir():
            print('Abort. Path exists and is not a directory: '
                  '{}'.format(parent))
            return
    if not dirpath.exists():
        print('Directory does not exist, creating: {}'.format(dirpath))
        if not dry_run:
            dirpath.mkdir(parents=True)
    for keyname in ['entityd.key', 'entityd.key_secret']:
        if dirpath.joinpath(keyname).exists() and not force:
            print('Abort. Key exists: {}'.format(dirpath.joinpath(keyname)))
            print('Use --force to overwrite any existing keys.')
            return
    if not dry_run:
        zmq.auth.create_certificates(str(dirpath), 'entityd')
    print('Created entityd.key and entityd.key_secret in {}'.format(dirpath))
