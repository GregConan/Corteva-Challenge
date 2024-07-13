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
import os
import shutil
import sys
from typing import Any, Dict, Iterable, List, Mapping, NamedTuple, Optional, Set, Union

# PyPI imports
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import psycopg2
import sqlalchemy as sa
from sqlalchemy import orm


# Define basic SQLAlchemy database object to modify
class Base(orm.DeclarativeBase):
    pass
db = SQLAlchemy(model_class=Base)  # db = SQLAlchemy(app,


# def define_models(db: SQLAlchemy) -> Flask  #?
class DailyWeatherReport(db.Model):
    # Flask-SQLAlchemy fields
    __tablename__ = "daily_weather_report"
    id: orm.Mapped[int] = orm.mapped_column(primary_key=True)

    # Fields specific to this DB Table
    max_temp: orm.Mapped[float] = orm.mapped_column(db.Float(precision=1))
    min_temp: orm.Mapped[float] = orm.mapped_column(db.Float(precision=1))
    date: orm.Mapped[dt.date] = orm.mapped_column(db.Date, nullable=False)  # [sa.types.Date]
    location: orm.Mapped[str] = orm.mapped_column(db.String(20),
                                                  nullable=False)  # TODO String(11) because location (filename) is only 11 chars long?
    precipitation: orm.Mapped[int] = orm.mapped_column(db.Integer)


    def __init__(self, max_temp, min_temp, date, location, precipitation) -> None:
        self.max_temp = max_temp
        self.min_temp = min_temp
        self.date = date
        self.location = location
        self.precipitation = precipitation


    def __repr__(self) -> str:
        return (f"<{self.min_temp}C to {self.max_temp}C with "
                f"{self.precipitation}cm precip at "
                f"{self.location} on {self.date}>")


class YearlyAvgWeatherReport(db.Model):  # TODO Abstract the fields/methods/etc. shared with DailyWeatherReport into a "WeatherReport" parent class for both to subclass?
    __tablename__ = "yearly_avg_weather_report"

    max_temp: orm.Mapped[float] = orm.mapped_column(db.Float)
    min_temp: orm.Mapped[float] = orm.mapped_column(db.Float)
    total_precip: orm.Mapped[int] = orm.mapped_column(db.Integer)
    year: orm.Mapped[int] = orm.mapped_column(db.Integer)

    def __init__(self, max_temp, min_temp, total_precip, year) -> None:
        self.max_temp = max_temp
        self.min_temp = min_temp
        self.total_precip = total_precip
        self.year = year

    def __repr__(self) -> str:
        return (f"<{self.min_temp}C to {self.max_temp}C on average with "
                f"{self.total_precip}cm precipitation in {self.year}>")


def create_Flask_app() -> Flask:  # (db: SQLAlchemy)
    # Create Flask app to attach to PostgreSQL DB
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = "postgresql://postgres:postgres@localhost:5432/api"  # TODO define API

    # Attach Flask app to PostgreSQLAlchemy DB object
    db.init_app(app)

    # Run app (?)
    with app.app_context():
        db.create_all()

        db.session.add(DailyWeatherReport())  # TODO Pass in the required parameters
        db.session.commit()

        reports = db.session.execute(db.select(DailyWeatherReport)).scalars()  # TODO Replace with command that makes sense in this context
    
    return app
