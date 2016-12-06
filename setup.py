"""Distutils build script for entityd."""

import setuptools


__version__ = '0.13.1'


with open("README.rst") as fp:
    LONG_DESCRIPTION = fp.read()


setuptools.setup(
    name='entityd',
    version=__version__,
    author='cobe.io',
    author_email='info@cobe.io',
    license='LGPLv3',
    url='https://bitbucket.org/cobeio/act',
    description=' Entity monitoring agent for cobe.io',
    long_description=LONG_DESCRIPTION,
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
        'requests >=2.11.1',
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
