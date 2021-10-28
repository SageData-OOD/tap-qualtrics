#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-qualtrics",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_qualtrics"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python==5.12.2",
        "requests",
        "pandas"
    ],
    entry_points="""
    [console_scripts]
    tap-qualtrics=tap_qualtrics:main
    """,
    packages=["tap_qualtrics"],
    package_data = {
        "schemas": ["tap_qualtrics/schemas/*.json"]
    },
    include_package_data=True,
)
