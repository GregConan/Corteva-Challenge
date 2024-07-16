#!/usr/bin/env python3
# coding: utf-8

"""
Greg Conan: gregmconan@gmail.com
Created: 2024-07-11
Updated: 2024-07-15
"""
# Import standard libraries
import sys

# Local custom imports
from corteva_challenge.utilities import ShowTimeTaken
from corteva_challenge import create_Flask_app, db


with ShowTimeTaken(f"running {sys.argv[0]}"):
    app = create_Flask_app()
