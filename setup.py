"""Distutils build script for entityd."""

import setuptools


def get_version():
    """Retrieve the version number from version.txt."""

    version = None
    with open('version.txt') as version_file:
        lines = version_file.readlines()
        if len(lines) > 0:
            version = lines[0]
        else:
            raise ValueError('Version number not found in version.txt')

    return version


with open("README.rst") as fp:
    LONG_DESCRIPTION = fp.read()


setuptools.setup(
    name='entityd',
    version=get_version(),
    author='cobe.io',
    author_email='info@cobe.io',
    license='LGPLv3',
    url='https://bitbucket.org/cobeio/act',
    description='Entity monitoring agent for cobe.io',
    long_description=LONG_DESCRIPTION,
    packages=setuptools.find_packages(),
    entry_points={
        'console_scripts': [
            'entityd=entityd.__main__:main',
            'entityd-health-check=entityd.health:check',
        ],
    },
    install_requires=[
        'setuptools',
        'Logbook',
        'syskit',
        'requests >=2.11.1',
        'pyyaml',
        'kube ==0.10.0',
        'python-cobe',
        'cobe-act',
        'docker-py',
        # TODO: Fix in python-cobe maybe?
        'pyzmq >=14.7, <=15.2',
        'msgpack-python >=0.4.5, <0.5',
        'voluptuous >=0.8.7, <0.9',
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
