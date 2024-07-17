#!/usr/bin/env python3
# coding: utf-8

"""
Greg Conan: gregmconan@gmail.com
Created: 2024-07-12
Updated: 2024-07-16
"""
# Import standard libraries
from collections.abc import Callable
import datetime as dt
import logging
import requests
from typing import Any, Mapping, Optional


def as_HTTPS_URL(*parts: str) -> str:
    """
    Re-usable convenience function to build URLs
    :param parts: Iterable[str] of slash-separated URL path parts
    :return: String, full HTTPS URL path
    """
    return "https://" + "/".join(parts)


def as_unit_or_null(col_name: str, as_num: Callable, row: Mapping[str, str],
                    unit_scale_factor: float) -> Any:
    """
    :param col_name: String naming the row key mapped to the value to convert
    :param as_num: Callable (e.g. type) to convert a string into a number
    :param row: Mapping[str, str] of column names to their values in a row
    :param unit_scale_factor: Float, multiple of 10 to ensure that the field
                              value returned is at the right scale (e.g. 0.1
                              converts 100 tenths of a degree into 10 degrees)
    :return: Object, a number (or null/None) formatted for its DB column
    """
    return (as_num(row[col_name].strip()) * unit_scale_factor
            if row[col_name] is not None else None)


def build_endpt_path(*path: str, **url_params: Any) -> str:
    """
    :param path: Iterable[str] of slash-separated API path parts
    :param url_params: Mapping[str, Any] of variable names and their values 
                       to filter data in response
    """
    str_params = [f"{k}={v}" for k, v in url_params.items()]
    return f"/{'/'.join(path)}?{'&'.join(str_params)}"


def download_GET(path_URL: str, headers: Mapping[str, Any]) -> Any:
    """
    :param path_URL: String, full URL path to a file/resource to download
    :param headers: Mapping[str, Any] of header names to their values in the
                    HTTP GET request to send to path_URL
    :return: Object(s) retrieved from path_URL using HTTP GET request
    """
    # Make the request to the API
    response = requests.get(path_URL, headers=headers)

    # Check if the request was successful
    try:
        assert response.status_code == 200
        return response
    except (AssertionError, requests.JSONDecodeError) as e:
        # TODO replace print with log
        print(f"\nFailed to retrieve file(s) at {path_URL}\n"
              f"{response.status_code} Error: {response.reason}")


# TODO Replace "print()" calls with "log()" calls after making log calls
#      display in the Debug Console window when running pytest tests
def log(content: str, level: int = logging.INFO) -> None:
    """
    :param content: String, the message to log/display
    :param level: int, the message's importance/urgency/severity level as
                  defined by logging module's 0 (ignore) to 50 (urgent) scale
    """
    logging.getLogger(__name__).log(msg=content, level=level)


def utcnow() -> dt.datetime:
    """
    :return: datetime.datetime, the moment that this function was called,
             set to the UTC timezone
    """
    return dt.datetime.now(tz=dt.timezone.utc)


class ShowTimeTaken:
    # TODO Use "log" instead of "print" by default
    def __init__(self, doing_what: str, show: Callable = print) -> None:
        """
        Context manager to time and log the duration of any block of code 
        :param doing_what: String describing what is being timed
        :param show: Function to print/log/show messages to the user
        """
        self.doing_what = doing_what
        self.show = show

    def __call__(self):
        """
        Explicitly defining __call__ as a no-op to prevent instantiation
        """
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
