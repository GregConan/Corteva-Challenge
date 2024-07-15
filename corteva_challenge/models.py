#!/usr/bin/env python3
# coding: utf-8

"""
Greg Conan: gregmconan@gmail.com
Created: 2024-07-12
Updated: 2024-07-14
"""
# Import standard libraries
import logging
import argparse
from collections.abc import Callable, Hashable
import csv
import datetime as dt
from glob import glob
import json
import os
import io
import pdb
import shutil
import sys
from typing import Any, Dict, Iterable, List, Mapping, NamedTuple, Optional, Set, Union

# PyPI imports
from flask import Flask
from flask_sqlalchemy.pagination import Pagination
from flask_sqlalchemy import SQLAlchemy
import dask.dataframe as dd
import psycopg2
import sqlalchemy as sa
from sqlalchemy import orm, and_
from sqlalchemy.dialects.postgresql import insert

# Local custom imports
from corteva_challenge.utilities import as_HTTPS_URL, download_GET, utcnow


# Define basic SQLAlchemy database object to modify
db = SQLAlchemy()  # db = SQLAlchemy(app,


# def define_models(db: SQLAlchemy) -> Flask  #?


class DBTable:
    id: orm.Mapped[int] = db.Column(
        db.Integer, primary_key=True, autoincrement=True)
    created: orm.Mapped[dt.datetime] = db.Column(db.DateTime, default=utcnow)


class DimensionTable(DBTable):
    updated: orm.Mapped[dt.datetime] = db.Column(db.DateTime, default=utcnow,
                                                 onupdate=utcnow)


class OnlineDataFile:
    def __init__(self, name: str, path: str, download_fn: Callable) -> None:
        self.name = name
        self.path = path
        self.download = download_fn

    def download_and_read(self):
        return self.download(self.path).text


class GitHubRepoAPI:
    ORIGIN = "api.github.com"

    def __init__(self, auth_token: str, name: str, owner: str,
                 data_subdirs: List[str],
                 endpt_type: Optional[str] = "repos",
                 data_to_get: Optional[str] = "contents") -> None:

        # HTTP request headers to download data files
        self.headers = {"Authorization": f"Bearer {auth_token}",
                        "Accept": "application/vnd.github.raw+json"}

        # (First part of) GitHub API endpoint URL
        self.URL_parts = [self.ORIGIN, endpt_type, owner, name, data_to_get]
        self.files_in = {subdir: self.get_data_files_in(subdir)
                         for subdir in data_subdirs}

    def download(self, path: str) -> Any:
        return download_GET(path, self.headers)

    def get_data_files_in(self, subdir: str) -> List[OnlineDataFile]:
        return [OnlineDataFile(metadata["name"], metadata["download_url"],
                               self.download) for metadata in
                self.download(as_HTTPS_URL(*self.URL_parts, subdir)).json()]


class WeatherReport(db.Model, DBTable):

    __table_args__ = (
        db.UniqueConstraint('station_id', 'date', name='uix_station_date'),
    )
    FIELDS = ("date", "max_temp", "min_temp", "precipitation")

    # Fields specific to this DB Table
    max_temp: orm.Mapped[float] = db.Column(db.Float(precision=1))
    min_temp: orm.Mapped[float] = db.Column(db.Float(precision=1))
    date: orm.Mapped[dt.date] = db.Column(
        db.Date, nullable=False)  # [sa.types.Date]
    precipitation: orm.Mapped[int] = db.Column(db.Integer)

    station_id = db.Column(db.Integer, db.ForeignKey("weather_station.id"),
                           nullable=False)

    def __repr__(self) -> str:
        return (f"<{self.min_temp}C to {self.max_temp}C with "
                f"{self.precipitation}cm precip at "
                f"{self.location} on {self.date}>")

    def to_dict(self) -> Dict[str, Any]:
        result = {field: getattr(self, field) for field in self.FIELDS}
        result["station_id"] = self.station_id
        result["id"] = self.id
        return result

    @classmethod
    def convert_data_in(cls, row: Dict[str, str], station_id: int) -> Dict[str, Any]:
        row = {k: None if v.strip() == "-9999" else v for k, v in row.items()}
        return {"station_id": station_id,
                "date": dt.datetime.strptime(row.pop("date").strip(), "%Y%m%d").date(),
                "max_temp": float(row["max_temp"].strip())/10 if isnt_nothing(row["max_temp"]) else None,
                "min_temp": float(row["min_temp"].strip())/10 if isnt_nothing(row["min_temp"]) else None,
                "precipitation": int(row["precipitation"].strip())*100 if isnt_nothing(row["precipitation"]) else None}

    @classmethod
    def run_page_query(cls, station_id: Optional[int] = None,
                       max_date: Optional[dt.date] = None,
                       min_date: Optional[dt.date] = None,
                       page: int = 1, per_page: int = 50) -> Pagination:
        conditions = list()
        if max_date:
            conditions.append(cls.date <= max_date)
        if min_date:
            conditions.append(cls.date >= min_date)
        if station_id:
            conditions.append(cls.station_id == station_id)
        filter_stmt = cls.query.filter(and_(True, *conditions))
        return filter_stmt.paginate(
            page=page, per_page=per_page, max_per_page=100, error_out=False
        )

    @classmethod
    def run_math_query_on(cls, col_name: str, math_fn: Callable) -> Any:
        return db.session.execute(math_fn(getattr(cls, col_name))).scalar()


def isnt_nothing(thing):
    return thing is not None


class WeatherStation(db.Model, DimensionTable):
    station_name: orm.Mapped[str] = db.Column(db.String(20), nullable=False,
                                              unique=True)  # TODO Is this needed?
    reports = db.relationship("WeatherReport", backref="weather_report",
                              lazy=True)  # TODO Verify that this uses "backref" rightly

    @classmethod
    # -> "WeatherStation":
    def load_reports_from(cls, station_file: OnlineDataFile):
        # def load_reports(cls, tsv_name: str, tsv_contents: str):

        # Prevent addition of duplicate stations
        station_name = os.path.splitext(station_file.name)[0]
        station_upsert = insert(cls).values(
            station_name=station_name).on_conflict_do_nothing(
            index_elements=['station_name']
        ).returning(cls.id)
        result = db.session.execute(station_upsert)
        db.session.commit()
        station_id = result.scalar()
        if not station_id:
            station_id = db.session.query(cls).filter_by(
                station_name=station_name).scalar().id

        # Download station data
        tsv_contents = station_file.download_and_read()

        # with io.StringIO(tsv_contents, newline="\n") as infile:
        reader = csv.DictReader(tsv_contents.split("\n"), delimiter="\t",
                                fieldnames=WeatherReport.FIELDS,
                                lineterminator="\n")
        station_reports = [WeatherReport.convert_data_in(row, station_id)
                           for row in reader]

        # Update metrics on matching station / date
        report_values = insert(WeatherReport).values(station_reports)
        report_upsert = report_values.on_conflict_do_update(
            index_elements=['station_id', 'date'],
            set_=dict(
                max_temp=report_values.excluded.max_temp,
                min_temp=report_values.excluded.min_temp,
                precipitation=report_values.excluded.precipitation,
            )
        )
        db.session.execute(report_upsert)
        db.session.commit()

        # pdb.set_trace()
        # [x for x in csv.DictReader(tsv_contents.split("\n"), fieldnames=["date", "max_temp", "min_temp", "precipitation"], delimiter="\t")]


class CropYield(db.Model, DBTable):
    year: orm.Mapped[int] = db.Column(db.Integer, nullable=False, unique=True)
    corn_bushels: orm.Mapped[int] = db.Column(db.Integer, nullable=False)

    @classmethod
    def load_yields_from(cls, yield_file: OnlineDataFile):
        tsv_name = yield_file.name
        tsv_contents = yield_file.download_and_read()
        reader = csv.DictReader(tsv_contents.split("\n"), fieldnames=[
            "year", "corn_bushels"
        ], delimiter="\t", lineterminator="\n")  # TODO OPTIMIZE

        yields = [{k: int(v.strip())
                   for k, v in x.items()} for x in reader]  # TODO OPTIMIZE
        yield_values = insert(cls).values(yields)
        yield_upsert = yield_values.on_conflict_do_update(
            index_elements=['year'],
            set_=dict(
                corn_bushels=yield_values.excluded.corn_bushels,
            )
        )
        db.session.execute(yield_upsert)
        db.session.commit()
