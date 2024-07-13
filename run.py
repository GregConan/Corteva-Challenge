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
from src.utilities import ShowTimeTaken
from src.orm import create_Flask_app


def main():
    app = create_Flask_app()


if __name__ == "__main__":
    with ShowTimeTaken(f"running {sys.argv[0]}"):
        main()

