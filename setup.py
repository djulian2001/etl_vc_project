#!/usr/bin/env python

try:
	from setuptools import setup
except ImportError:
	from distutils.core import setup

config = {
	'description': 'Extract Translate Load ASU people soft data from the directory warehouse into the biodesign database(s) for use, reporting, etc.',
    'author': 'David Julian',
    'url': 'https://git.biodesign.asu.edu/primusdj/asuToBiodesign/tree/master',
    'download_url': 'https://git.biodesign.asu.edu/primusdj/asuToBiodesign.git',
    'author_email': 'david.julian@asu.edu',
    'version': '0.1',
    'install_requires': [
        "sqlalchemy",
        "alembic",
        "cx_oracle",
        "MySQL-python",
    ],
    'packages': ["bioetl","models","app","alembic"],
    'scripts': [],
    'name': 'asuToBiodesign'
}

setup(**config)
