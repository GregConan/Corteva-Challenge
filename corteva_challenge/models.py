#!/usr/bin/env python3
# coding: utf-8

"""
Greg Conan: gregmconan@gmail.com
Created: 2024-07-12
Updated: 2024-07-15
"""
# Import standard libraries
from collections.abc import Callable
import csv
import datetime as dt
import os
from typing import Any, Dict, List, Mapping, Optional

# PyPI imports
from flask import jsonify
from flask_sqlalchemy.pagination import Pagination
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import and_, ColumnExpressionArgument, orm
from sqlalchemy.dialects.postgresql import insert

# Local custom imports
from corteva_challenge.utilities import (as_HTTPS_URL, as_unit_or_null,
                                         download_GET, utcnow)


# Define basic SQLAlchemy database object to modify
db = SQLAlchemy()


class DBTable:
    """
    PostgreSQL database Table with at least 2 columns, unique int ID and 
    date-timestamp of the moment of each row's creation in the database.
    Defined to consolidate code shared/redundant between Model classes below.
    """
    id: orm.Mapped[int] = db.Column(
        db.Integer, primary_key=True, autoincrement=True)
    created: orm.Mapped[dt.datetime] = db.Column(db.DateTime, default=utcnow)

    @classmethod
    def select_page(cls, *conditions: ColumnExpressionArgument[bool],
                    page: int = 1, per_page: int = 50) -> Pagination:
        """
        Get 1 page of data SELECTed from DBTable with optional filter
        conditions specified to exclude certain data
        :param conditions: Iterable[ColumnExpressionArgument[bool]]
        :param page: Int, the page number of query results to return
        :param per_page: Int, number of query result rows per returned page
        :return: Pagination, the filtered query results page
        """
        return cls.query.filter(and_(True, *conditions)).paginate(
            page=page, per_page=per_page, max_per_page=100, error_out=False
        )

    @classmethod
    def get_pagination_JSON(cls, request_args, **field_types) -> Dict[str, Any]:
        """ 
        Run a SELECT query on the DBTable and return requested rows
        :return: Dict[str, Any] of JSON data mapping "items" to a list of dicts
                mapping DBTable field/column names to their values in all rows
                that match the specified filter conditions
        """
        params = dict(
            page=request_args.get("page", type=int, default=1),
            per_page=request_args.get("per_page", type=int, default=50),
        )
        for field_name, field_type in field_types.items():
            params[field_name] = request_args.get(field_name, type=field_type)
        result_page = cls.run_page_query(**params)
        return jsonify(page=result_page.page,
                       items=[row.to_dict() for row in result_page.items],
                       total=result_page.total, next=result_page.next_num)

    def to_dict(self):
        raise NotImplementedError(f"{self.__class__.__name__} needs to "
                                  "implement to_dict()")

    @classmethod
    def run_page_query(cls, page: int = 1, per_page: int = 50) -> Pagination:
        """
        Get 1 page of data SELECTed from DBTable
        :param page: Int, the page number of query results to return
        :param per_page: Int, number of query result rows per returned page
        :return: Pagination, the query results page
        """
        return cls.select_page(page=page, per_page=per_page)


class DimensionTable(DBTable):
    """
    DBTable extension with at least 1 other column, date-timestamp of the
    moment that a given row was last updated.
    Defined to consolidate code shared/redundant between Model classes below.
    """
    updated: orm.Mapped[dt.datetime] = db.Column(db.DateTime, default=utcnow,
                                                 onupdate=utcnow)


class OnlineDataFile:
    """
    File existing online (namely in a GitHub repo) with data to download.
    Defined to consolidate code shared/redundant between GitHubRepoAPI methods
    and Model methods below.
    """

    def __init__(self, name: str, path: str, download_fn: Callable) -> None:
        """
        :param name: String, the exact filename including its extension.
        :param path: String, the URL path at which the file exists and (more
                     importantly) can be downloaded from.
        :param download_fn: Callable, function which accepts the download URL
                            as an input argument and downloads this file.
        """
        self.name = name
        self.path = path
        self.download = download_fn

    def download_and_read(self) -> str:
        """
        Download this file and read its contents.
        :return: String, all text contents of this OnlineDataFile.
        """
        return self.download(self.path).text


class GitHubRepoAPI:
    ORIGIN = "api.github.com"

    def __init__(self, auth_token: str, name: str, owner: str,
                 data_subdirs: List[str],
                 endpt_type: Optional[str] = "repos",
                 data_to_get: Optional[str] = "contents") -> None:
        """
        :param auth_token: String, entire valid GitHub authentication token to
                           access the GitHub API using REST requests
        :param name: String, the exact name of this GitHub repository
        :param owner: String, the exact name of the GitHub user who owns this
                      GitHub repository
        :param data_subdirs: List[str] of subdirectory relative paths that 
                             contain data text files to download all of
        :param endpt_type: String, the first part of the GitHub API relative
                           URL path defining what kind of object to access
        :param data_to_get: String, a later part of the GitHub API relative
                            URL path defining what aspect/attribute of the 
                            given endpt_type/object to access
        """
        # HTTP request headers to download data files
        self.headers = {"Authorization": f"Bearer {auth_token}",
                        "Accept": "application/vnd.github.raw+json"}

        # (First part of) GitHub API endpoint URL
        self.URL_parts = [self.ORIGIN, endpt_type, owner, name, data_to_get]

        # Use GET request to read and store the URLs of the files to download
        self.files_in = {subdir: self.get_data_files_in(subdir)
                         for subdir in data_subdirs}

    def download(self, path: str) -> Any:
        """
        :param path: String, the URL path of the data file to download
        :return: Object downloaded from path using a GET request
        """
        return download_GET(path, self.headers)

    def get_data_files_in(self, subdir: str) -> List[OnlineDataFile]:
        """
        :param subdir: String, relative path to this GitHub repo subdirectory
                       containing data files to download
        :return: List[OnlineDataFile] ready to download from this repo
        """
        return [OnlineDataFile(metadata["name"], metadata["download_url"],
                               self.download) for metadata in
                self.download(as_HTTPS_URL(*self.URL_parts, subdir)).json()]


class WeatherReport(db.Model, DBTable):
    """
    weather_report PostgreSQL DBTable represented in ORM for data access
    """
    __table_args__ = (  # Each WeatherStation has only one report per day
        db.UniqueConstraint("station_id", "date", name="uix_station_date"),
    )
    # Numeric (non-metadata) fields/columns of any given weather data report
    FIELDS = ("date", "max_temp", "min_temp", "precipitation")

    # Fields specific to this DB Table
    max_temp: orm.Mapped[float] = db.Column(db.Float(precision=1))
    min_temp: orm.Mapped[float] = db.Column(db.Float(precision=1))
    date: orm.Mapped[dt.date] = db.Column(db.Date, nullable=False)
    precipitation: orm.Mapped[int] = db.Column(db.Integer)

    station_id = db.Column(db.Integer, db.ForeignKey("weather_station.id"),
                           nullable=False)

    def __repr__(self) -> str:
        return (f"<{self.min_temp}C to {self.max_temp}C with "
                f"{self.precipitation}cm precip at "
                f"{self.location} on {self.date}>")

    def to_dict(self) -> Dict[str, Any]:
        """
        Get a DB row as a dict
        :return: Dict[str, Any] mapping column names to their values in a
                 given row of the weather_report PostgreSQL DBTable
        """
        result = {field: getattr(self, field) for field in self.FIELDS}
        result["station_id"] = self.station_id
        result["id"] = self.id
        return result

    @classmethod
    def convert_data_in(cls, row: Mapping[str, str],
                        station_id: int) -> Dict[str, Any]:
        """
        Transform downloaded text data into the correct format to store in the
        weather_report PostgreSQL DBTable: identify nulls and fix types
        :param row: Mapping[str, str] of column names to their values in a row
        :param station_id: Int uniquely identifying the WeatherStation that
                           this WeatherReport is from
        :return: Dict[str, Any], a row ready to add to the weather_report
                 PostgreSQL DBTable
        """
        row = {k: None if v.strip() == "-9999" else v for k, v in row.items()}
        return {"station_id": station_id,
                "date": dt.datetime.strptime(row.pop("date").strip(), "%Y%m%d").date(),
                "max_temp": as_unit_or_null("max_temp", float, row, 0.1),
                "min_temp": as_unit_or_null("min_temp", float, row, 0.1),
                "precipitation": as_unit_or_null("precipitation", int, row, 100)}

    @classmethod
    def run_page_query(cls, station_id: Optional[int] = None,
                       max_date: Optional[dt.date] = None,
                       min_date: Optional[dt.date] = None,
                       page: int = 1, per_page: int = 50) -> Pagination:
        """
        Get 1 page of data SELECTed from weather_report DBTable with optional
        filter conditions specified
        :param station_id: Int uniquely identifying the WeatherStation that
                           this WeatherReport is from
        :param max_date: datetime.Date after which to exclude WeatherReport
                         rows from the query result
        :param max_date: datetime.Date before which to exclude WeatherReport
                         rows from the query result
        :param page: Int, the page number of query results to return
        :param per_page: Int, number of query result rows per returned page
        :return: Pagination, the date-/station-filtered query results page
        """
        conditions = list()
        if max_date is not None:
            conditions.append(cls.date <= max_date)
        if min_date is not None:
            conditions.append(cls.date >= min_date)
        if station_id is not None:
            conditions.append(cls.station_id == station_id)
        return cls.select_page(*conditions, page=page, per_page=per_page)

    @classmethod
    def run_math_query_on(cls, col_name: str, math_fn: Callable) -> Any:
        """
        :param col_name: String naming the numerical column in the
                         weather_report DBTable to run a statistics query on
        :param math_fn: Callable, function that accepts a column of numerical
                        data and calculates a statistical value to return
        :return: Object, the numerical result of math_fn or None
        """
        return db.session.execute(math_fn(getattr(cls, col_name))).scalar()


class WeatherStation(db.Model, DimensionTable):
    """
    weather_station PostgreSQL DBTable represented in ORM for data access
    """
    station_name: orm.Mapped[str] = db.Column(db.String(20), nullable=False,
                                              unique=True)  # TODO Is this needed?
    reports = db.relationship("WeatherReport", backref="weather_report",
                              lazy=True)

    @classmethod
    def load_reports_from(cls, station_file: OnlineDataFile) -> None:
        """
        Given the path to a text file containing rows of data from this 
        WeatherStation, download that file, extract its contents, transform
        them into the right format, and add them to the PostgreSQL database
        as a new DBTable
        :param station_file: OnlineDataFile to download, extract weather 
                             station data (in .tsv text format) from, and
                             load that data from into the DBTable
        """
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

    def to_dict(self) -> Dict[str, Any]:
        return {"station_name": self.station_name,  # "reports": self.reports,
                "id": self.id, "created": self.created,
                "updated": self.updated}


class CropYield(db.Model, DBTable):
    """
    crop_yield PostgreSQL DBTable represented in ORM for data access
    """
    year: orm.Mapped[int] = db.Column(db.Integer, nullable=False, unique=True)
    corn_bushels: orm.Mapped[int] = db.Column(db.Integer, nullable=False)

    @classmethod
    def load_yields_from(cls, yield_file: OnlineDataFile) -> None:
        """
        Given the path to a text file containing rows of yearly CropYield
        data, download that file, extract its contents, transform them into
        the right format, and add them as a new PostgreSQL DBTable
        :param station_file: OnlineDataFile to download, extract crop yield 
                             data (in .tsv text format) from, and load that
                             data from into the DBTable
        """
        tsv_name = yield_file.name  # TODO Is this needed?
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

    def to_dict(self) -> Dict[str, Any]:
        return {"corn_bushels": self.corn_bushels, "year": self.year,
                "id": self.id, "created": self.created}
