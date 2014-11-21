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
    license='Proprietary',
    url='http://abilisoft.com',
    description='Abilisoft Entity Monitoring Agent',
    packages=['entityd'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: Other/Proprietary License',
    ],
    keywords='topology performance availability monitoring',
)
