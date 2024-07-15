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
import pdb
import requests
import shutil
import sys
from typing import Any, Dict, Iterable, List, Mapping, NamedTuple, Optional, Set, Union


def as_HTTPS_URL(*parts: str) -> str:
    return "https://" + "/".join(parts)


def download_GET(path_URL: str, headers: Dict[str, Any]) -> Any:

    # Make the request to the GitHub API
    response = requests.get(path_URL, headers=headers)

    # Check if the request was successful
    try:
        assert response.status_code == 200
        return response
    except (AssertionError, requests.JSONDecodeError) as e:
        print(f"\nFailed to retrieve file(s) at {path_URL}\n"
              f"{response.status_code} Error: {response.reason}")


def log(content: str, level=logging.INFO) -> None:
    logging.getLogger(__name__).log(msg=content, level=level)


def utcnow() -> dt.datetime:
    return dt.datetime.now(tz=dt.timezone.utc)


class ShowTimeTaken:
    # TODO Use Python's "logging" library to define "log" function below
    def __init__(self, doing_what: str, show: Callable = print) -> None:
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
        self.start = dt.datetime.now()
        self.show(f"Started {self.doing_what} at {self.start}")
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
