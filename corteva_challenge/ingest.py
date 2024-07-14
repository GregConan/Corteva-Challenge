#!/usr/bin/env python3
# coding: utf-8

"""
Greg Conan: gregmconan@gmail.com
Created: 2024-07-13
Updated: 2024-07-13
"""
# Import standard libraries
import os
import pdb
from typing import Any, Dict, Iterable, List, Mapping, NamedTuple, Optional, Set, Union

# PyPI imports
import dask.dataframe as dd
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import psycopg2
import sqlalchemy
from sqlalchemy import orm

# Local imports
from corteva_challenge.models import WeatherStation
from corteva_challenge.config import (DATA_SRC_GITHUB_REPO_NAME,
                                      DATA_SRC_GITHUB_REPO_OWNER)
from corteva_challenge.utilities import as_HTTPS_URL, download_GET


class GitHubRepoAPI:
    ORIGIN = "api.github.com"

    def __init__(self, auth_token: str, name: str, owner: str,
                 data_subdirs: List[str],
                 endpt_type: str = "repos",
                 data_to_get: str = "contents") -> None:

        # HTTP request headers to download data files
        self.headers = {"Authorization": f"Bearer {auth_token}",
                        "Accept": "application/vnd.github.raw+json"}

        # (First part of) GitHub API endpoint URL
        self.URL_parts = [self.ORIGIN, endpt_type, owner, name, data_to_get]

        self.file_URLs_in = dict()
        for subdir in data_subdirs:
            self.file_URLs_in[subdir] = self.list_file_URLs_in(subdir)

    def list_file_URLs_in(self, subdir: str) -> List[str]:
        return {file_metadata["name"]: file_metadata.get("download_url")
                for file_metadata in
                self.download(as_HTTPS_URL(*self.URL_parts, subdir)).json()}

    def download(self, path: str):

        return download_GET(path, self.headers)


def ingest(gh_token):
    DAILY_WEATHER_SUBDIR = "wx_data"
    YEARLY_YIELD_SUBDIR = "yld_data"

    repo = GitHubRepoAPI(auth_token=gh_token,
                         name=DATA_SRC_GITHUB_REPO_NAME,
                         owner=DATA_SRC_GITHUB_REPO_OWNER,
                         data_subdirs=[DAILY_WEATHER_SUBDIR,
                                       YEARLY_YIELD_SUBDIR])

    stations = list()
    for station_name, file_URL in \
            repo.file_URLs_in[DAILY_WEATHER_SUBDIR].items():

        stations.append(WeatherStation.load_reports(
            station_name, repo.download(file_URL).text
        ))
