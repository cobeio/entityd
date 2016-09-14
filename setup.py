"""Distutils build script for entityd."""

import io

import setuptools


__version__ = '0.13.0'


def get_long_description():
    """Generate a long description from the README file."""
    descr = []
    for fname in ('README.rst',):
        with io.open(fname, encoding='utf-8') as file:
            descr.append(file.read())
    return '\n\n'.join(descr)


setuptools.setup(
    name='entityd',
    version=__version__,
    author='cobe.io',
    author_email='info@cobe.io',
    license='LGPLv3',
    url='https://bitbucket.org/cobeio/act',
    description=' Entity monitoring agent for cobe.io',
    long_description=get_long_description(),
    packages=setuptools.find_packages(),
    entry_points={
        'console_scripts': [
            'entityd=entityd.__main__:main',
        ],
    },
    install_requires=[
        'setuptools',
        'msgpack-python',
        'Logbook',
        'pyzmq',
        'syskit',
        'requests',
        'pyyaml',
        'kube >=0.8.0,<0.10.0',
        'python-cobe',
        'cobe-act',
        'docker-py',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: '
        'OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
        'Topic :: Software Development :: Libraries',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    keywords='topology performance availability monitoring',
)
