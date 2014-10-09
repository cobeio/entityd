import pathlib
from distutils.core import setup


version_module = pathlib.Path(__file__).parent / 'entityd' / 'version.py'
version_ns = {}
with version_module.open() as fp:
    exec(fp.read(), version_ns)
version = version_ns['__version__']


setup(
    name='entityd',
    version=version,
    author='Abilisoft Ltd.',
    author_email='info@abilisoft.com',
    license='',
    url='http://abilisoft.com',
    description='Abilisoft Entity Monitoring Agent',
    packages=['entityd'],
    classifiers=[
        'Development Status :: 3 - Alpha',
    ],
    keywords='topology performance availablility monitoring',
)
