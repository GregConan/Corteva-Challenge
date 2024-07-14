#!/usr/bin/env python3
# coding: utf-8

"""
Greg Conan: gregmconan@gmail.com
Created: 2024-07-12
Updated: 2024-07-13
"""
# Import standard libraries
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
import dask.dataframe as dd
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import psycopg2
import sqlalchemy
from sqlalchemy import orm

# Local custom imports
from corteva_challenge.utilities import utcnow


# Define basic SQLAlchemy database object to modify
db = SQLAlchemy()  # db = SQLAlchemy(app,


# def define_models(db: SQLAlchemy) -> Flask  #?


class DBTable:
    id: orm.Mapped[int] = db.Column(primary_key=True, autoincrement=True)
    created: orm.Mapped[dt.datetime] = db.Column(db.DateTime, default=utcnow)


class DimensionTable(DBTable):
    updated: orm.Mapped[dt.datetime] = db.Column(db.DateTime, default=utcnow,
                                                 onupdate=utcnow)


class WeatherStation(db.Model, DimensionTable):
    station_name: orm.Mapped[str] = db.Column(db.String(20), nullable=False,
                                              unique=True)  # TODO Is this needed?
    reports = db.relationship("WeatherReport", backref="weather_report",
                              lazy=True)  # TODO Verify that this uses "backref" rightly

    @classmethod
    # -> "WeatherStation":
    def load_reports(cls, tsv_name: str, tsv_contents: str):

        station = WeatherStation(station_name=os.path.splitext(tsv_name)[0])
        db.session.add(station)
        db.session.commit()

        # with io.StringIO(tsv_contents, newline="\n") as infile:
        reader = csv.DictReader(tsv_contents.split("\n"), fieldnames=[
            "date", "max_temp", "min_temp", "precipitation"
        ], delimiter="\t", lineterminator="\n")  # TODO OPTIMIZE

        station_reports = list()
        for record in [{k: int(v.strip()) if k != "date" else
                        dt.datetime.strptime(v.strip(), "%Y%m%d")
                        for k, v in x.items()} for x in reader]:  # TODO OPTIMIZE

            station_reports.append(WeatherReport(
                station_id=station.id, **record))

        db.session.add_all(station_reports)
        db.session.commit()

        # pdb.set_trace()
        # [x for x in csv.DictReader(tsv_contents.split("\n"), fieldnames=["date", "max_temp", "min_temp", "precipitation"], delimiter="\t")]


class WeatherReport(db.Model, DBTable):

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
