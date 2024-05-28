#! /usr/bin/env python
# -*- coding: utf-8 -*-

from distutils.core import setup
from setuptools import find_packages

setup(
    name="davinci",
    version="0.1.0",
    # packages=find_packages('davinci'),
    packages=['davinci'],
    license="LICENSE",
    description="Package to generate all analytical solutions at Kenco",
    long_description=open("README.md").read(),
    install_requires=open("requirements.txt").read().split()
)