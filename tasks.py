"""Some simple task automatisation.

At a bare minimum one should make sure the ``check`` command reports
no issues at all when submitting a pullrequest.

"""

import xml.etree.ElementTree as etree

import invoke


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
