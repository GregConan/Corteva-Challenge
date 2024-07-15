#!/usr/bin/env python3
# coding: utf-8

"""
Greg Conan: gregmconan@gmail.com
Created: 2024-07-11
Updated: 2024-07-12
"""
# Import standard libraries
import argparse
from collections.abc import Callable, Hashable
import datetime as dt
from glob import glob
import json
import os
import shutil
import sys
from typing import Any, Dict, Iterable, List, Mapping, NamedTuple, Optional, Set, Union


# Local custom imports
from corteva_challenge.utilities import ShowTimeTaken
from corteva_challenge import create_Flask_app, db


with ShowTimeTaken(f"running {sys.argv[0]}"):
    app = create_Flask_app()
