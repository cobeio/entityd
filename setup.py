"""Distutils build script for entityd."""

import setuptools


def get_version():
    """Retrieve the version number from conda/meta.yaml."""
    with open('conda/entityd/meta.yaml') as fp:
        for line in fp:
            if 'set version =' in line:
                break
        else:
            raise ValueError('Version number not found in meta.yaml')
    try:
        version = line.split('=')[1].split()[0].strip('"\'')  # pylint: disable=undefined-loop-variable
    except Exception as err:
        raise ValueError('Failed to parse version from meta.yaml') from err
    return version


setuptools.setup(
    name='entityd',
    version=get_version(),
    author='Abilisoft Ltd.',
    author_email='info@abilisoft.com',
    license='Proprietary',
    url='http://abilisoft.com',
    description='Abilisoft Entity Monitoring Agent',
    packages=['entityd'],
    scripts=['bin/entityd'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: Other/Proprietary License',
    ],
    keywords='topology performance availability monitoring',
)
