#!/usr/bin/env python3
# coding: utf-8

"""
Greg Conan: gregmconan@gmail.com
Created: 2024-07-13
Updated: 2024-07-13
"""
# Import standard libraries
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp
import os
from typing import Any, Callable, Dict, Iterable, List, Mapping, NamedTuple, Optional, Set, Union

# Local imports
from corteva_challenge.config import (DATA_SRC_GITHUB_REPO_NAME,
                                      DATA_SRC_GITHUB_REPO_OWNER)
from corteva_challenge.models import CropYield, GitHubRepoAPI, WeatherStation
from corteva_challenge.utilities import ShowTimeTaken


def ingest(gh_token: str, max_files: Optional[int] = None):
    DAILY_WEATHER_SUBDIR = "wx_data"
    YEARLY_YIELD_SUBDIR = "yld_data"

    repo = GitHubRepoAPI(auth_token=gh_token,
                         name=DATA_SRC_GITHUB_REPO_NAME,
                         owner=DATA_SRC_GITHUB_REPO_OWNER,
                         data_subdirs=[DAILY_WEATHER_SUBDIR,
                                       YEARLY_YIELD_SUBDIR])

    get_files_from(repo, WeatherStation.load_reports_from,
                   DAILY_WEATHER_SUBDIR, max_files)
    get_files_from(repo, CropYield.load_yields_from,
                   YEARLY_YIELD_SUBDIR, max_files)


def get_files_from(repo: GitHubRepoAPI, load_method: Callable, subdir: str,
                   max_files: Optional[int] = None):
    n_usable_CPUs = len(os.sched_getaffinity(0))
    files = repo.files_in[subdir]
    if max_files is not None:
        files = files[:max_files]
    with ShowTimeTaken(f"processing {len(files)} files from {subdir}"):
        with ProcessPoolExecutor(max_workers=n_usable_CPUs - 1) as executor:
            processes = [p for p in executor.map(load_method, files)]
