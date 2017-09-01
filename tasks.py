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
def pylint(ctx):
    """Invoke pylint on modules and test code."""
    pylint_cmd = os.path.join(sys.prefix, "bin/pylint") + " -f parseable"
    output_cmd = "| tee -a results/pylint.log ; exit ${PIPESTATUS[0]}"

    tasks = [
        "entityd",
        "--rcfile tests/pylintrc tests/*.py",
        "setup.py",
        "tasks.py",
    ]

    # Remove the pylint log before running and make sure the results dir exists
    ctx.run("mkdir -p results && echo "" > results/pylint.log")

    results = list()
    for task in tasks:
        cmd = pylint_cmd + " " + task + " " + output_cmd
        result = ctx.run(cmd, warn=True)
        results.append(result.exited)

    if max(results) > 0:
        raise invoke.Exit(code=max(results))


@invoke.task
def pytest(ctx):
    """Run the entire test-suite."""
    ctx.run('py.test -q --cov-report=xml:results/coverage.xml tests')
    tree = etree.parse('results/coverage.xml')  # Probably should use sax.
    root = tree.getroot()
    total = float(root.get('line-rate', 0))
    print('Test coverage: {:d}%'.format(int(total*100)))
    if total < 1:
        ctx.run('false')     # Crappy way of making the task fail


@invoke.task
def jenkins_pytest(ctx):
    """Task jenkins uses to run tests"""
    pytest_args = [
        sys.prefix + '/bin/py.test',
        '-v -m "not non_container"',
        '--junitxml=results/test_results.xml',
        '--cov-report term-missing',
        '--cov-report xml:results/coverage.xml',
    ]
    res = ctx.run(' '.join(pytest_args))
    if res.exited > 0:
        raise invoke.Exit(code=res.exited)


@invoke.task(pre=[pylint, pytest])
def check(ctx):  # pylint: disable=unused-argument
    """Perform all checks."""


@invoke.task(help={'dirpath': 'The base path to install keys under. Keys will'
                              ' be installed at `<dirpath>/entityd/keys` with'
                              ' any necessary directories being created. If'
                              ' not provided the default is '
                              '`<env>/etc/entityd/keys`.',
                   'force': 'Force overwriting of existing keys.',
                   'dry-run': 'Do a dry-run without making any keys.'})
def certificates(ctx, dirpath=None, force=False, dry_run=False):  # pylint: disable=unused-argument
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
