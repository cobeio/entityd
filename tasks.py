"""Some simple task automatisation.

At a bare minimum one should make sure the ``check`` command reports
no issues at all when submitting a pullrequest.

"""

import xml.etree.ElementTree as etree

import invoke


@invoke.task
def pylint():
    """Invoke pylint on modules and test code."""
    print_msg('Invoking pylint')
    invoke.run('pylint entityd')
    invoke.run('cd tests; pylint *.py')
    invoke.run('setup.py')
    invoke.run('pylint tasks.py')


@invoke.task
def pytest():
    """Run the entire test-suite."""
    print_msg('Invoking py.test')
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
        pass
    else:
        err = None
    pytest()
    if err:
        raise err


def print_msg(msg, fill='+', width=80):
    """Print a message surrounded by fill character."""
    nchars = (width - (len(msg) + 2)) // 2
    print('{0} {1} {0}'.format(fill*nchars, msg))
