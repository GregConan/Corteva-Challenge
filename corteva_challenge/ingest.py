#!/usr/bin/env python3
# coding: utf-8

"""
Greg Conan: gregmconan@gmail.com
Created: 2024-07-13
Updated: 2024-07-16
"""
# Import standard libraries
from typing import Callable, Optional

# Local custom imports
from corteva_challenge.config import (DATA_SRC_GITHUB_REPO_NAME,
                                      DATA_SRC_GITHUB_REPO_OWNER)
from corteva_challenge.models import CropYield, GitHubRepoAPI, WeatherStation
from corteva_challenge.utilities import ShowTimeTaken


def ingest(gh_token: str, max_files: Optional[int] = None) -> None:
    """
    :param gh_token: String, entire valid GitHub authentication token to
                     access the GitHub API using REST requests
    :param max_files: Int, upper limit on the number of files to load at once
    """
    # Access GitHub repository containing data files to ingest
    DAILY_WEATHER_SUBDIR = "wx_data"
    YEARLY_YIELD_SUBDIR = "yld_data"
    repo = GitHubRepoAPI(auth_token=gh_token,
                         name=DATA_SRC_GITHUB_REPO_NAME,
                         owner=DATA_SRC_GITHUB_REPO_OWNER,
                         data_subdirs=[DAILY_WEATHER_SUBDIR,
                                       YEARLY_YIELD_SUBDIR])

    # Download and ingest the data files
    get_files_from(repo, WeatherStation.load_reports_from,
                   DAILY_WEATHER_SUBDIR, max_files)
    get_files_from(repo, CropYield.load_yields_from,
                   YEARLY_YIELD_SUBDIR, max_files)


def get_files_from(repo: GitHubRepoAPI, load_method: Callable, subdir: str,
                   max_files: Optional[int] = None) -> None:
    """
    :param repo: GitHubRepoAPI to download data text files from
    :param load_method: DBTable ETL classmethod which downloads a data file,
                        extracts the data, transforms it, and adds it to the
                        relevant PostgreSQL database
    :param subdir: String, relative path to the GitHub repo subdirectory of
                   data files to download 
    :param max_files: Int, upper limit on the number of files to load at once
    """
    files = repo.files_in[subdir]
    if max_files is not None:
        files = files[:max_files]
    with ShowTimeTaken(f"processing {len(files)} files from {subdir}"):
        for eachfile in files:  # TODO Parallelize without breaking AWS deploy
            load_method(eachfile)
