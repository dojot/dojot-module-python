# -*- coding: utf-8 -*-
"""
setup.py script
"""

import io
from collections import OrderedDict
from setuptools import setup, find_packages

with io.open('README.md', 'rt', encoding='utf8') as f:
    README = f.read()

setup(
    name='dojot.module',
    version='0.0.1-alpha.1',
    url='http://github.com/dojot/dojot-module-python',
    project_urls=OrderedDict((
        ('Code', 'https://github.com/dojot/dojot-module-python.git'),
        ('Issue tracker', 'https://github.com/dojot/dojot-module-python/issues'),
    )),
    license='GPL-3.0',
    author='Matheus Campanha Ferreira',
    author_email='campanha@cpqd.com.br',
    maintainer='dojot team',
    description='Library for new dojot modules development',
    long_description=README,
    packages=["dojot.module", "dojot.module.kafka"],
    include_package_data=True,
    zip_safe=False,
    platforms=[any],
    install_requires=[
        'requests==2.18.0',
        'kafka-python==1.4.3',
        'colorlog==3.1.4',
        'pyaml==17.12.1'
    ],
    extras_require={
        "dev": [
            "pytest>=3",
            "mock==2.0.0"
        ]
    }
)
