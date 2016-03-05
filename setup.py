import os
import re
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.md')).read()
CHANGES = open(os.path.join(here, 'CHANGES.txt')).read()

requires = [x.rstrip() for x in
    open(os.path.join(here, 'requirements.txt')).readlines()
    if not x.startswith('-e')]
tests_requires = requires

from pprint import pprint; pprint(tests_requires)

setup(
    name='Stoma',
    version='0.1',
    description='Parenchym Filesystem Indexer',
    long_description=README + '\n\n' + CHANGES,
    classifiers=[
    ],
    author='Dirk Makowski',
    author_email='johndoe@example.com',
    url='http://parenchym.com',
    keywords='',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=requires,
    tests_require=tests_requires,
    test_suite="stoma",
    entry_points="""\
      [console_scripts]
      stoma = stoma.scripts.stoma:main
      """,
)
