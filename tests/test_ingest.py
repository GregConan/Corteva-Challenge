#!/usr/bin/env python3
# coding: utf-8

"""
Greg Conan: gregmconan@gmail.com
Created: 2024-07-14
Updated: 2024-07-15
"""
# Import standard libraries
import os

# PyPI imports
from flask import Flask

# Local custom imports
from corteva_challenge.ingest import ingest
from corteva_challenge.utilities import ShowTimeTaken


def test_ingest(app: Flask) -> None:
    with ShowTimeTaken("testing the 'ingest' function"):
        ingest(os.getenv("GITHUB_TOKEN"), max_files=10)
