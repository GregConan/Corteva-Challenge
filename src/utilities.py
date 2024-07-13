#!/usr/bin/env python3
# coding: utf-8

"""
Greg Conan: gregmconan@gmail.com
Created: 2024-07-12
Updated: 2024-07-12
"""
# Import standard libraries
import argparse
from collections.abc import Callable, Hashable
import datetime as dt
from glob import glob
import json
import logging
import os
import shutil
import sys
from typing import Any, Dict, Iterable, List, Mapping, NamedTuple, Optional, Set, Union


class ShowTimeTaken:
    # TODO Use Python's "logging" library to define "log" function below
    def __init__(self, doing_what: str, show: Callable = log) -> None:
        """
        Context manager to time and log the duration of any block of code 
        :param doing_what: String describing what is being timed
        :param show: Function to print/log/show messages to the user
        """
        self.doing_what = doing_what
        self.show = show


    def __call__(self):
        pass


    def __enter__(self):
        """
        Log the moment that script execution enters the context manager and
        what it is about to do. 
        """
        self.show(f"Just started {self.doing_what}")
        self.start = dt.datetime.now()
        return self
    

    def __exit__(self, exc_type: Optional[type] = None,
                 exc_val: Optional[BaseException] = None, exc_tb=None):
        """
        Log the moment that script execution exits the context manager and
        what it just finished doing. 
        :param exc_type: Exception type
        :param exc_val: Exception value
        :param exc_tb: Exception traceback
        """
        self.elapsed = dt.datetime.now() - self.start
        self.show(f"\nTime elapsed {self.doing_what}: {self.elapsed}")