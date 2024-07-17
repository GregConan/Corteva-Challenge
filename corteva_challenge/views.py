#!/usr/bin/env python3
# coding: utf-8

"""
Greg Conan: gregmconan@gmail.com
Created: 2024-07-12
Updated: 2024-07-16
"""
# Import standard libraries
import datetime as dt
from typing import Any, Dict

# PyPI imports
from flask import Blueprint, jsonify, request
import sqlalchemy as sa

# Local custom imports
from corteva_challenge.models import (CropYield, db, WeatherReport,
                                      WeatherStation)


bp = Blueprint("weather", __name__, url_prefix="/api")


@bp.get("/weather")
def get_weather() -> Dict[str, Any]:
    """ Weather report data endpoint
    ---
    parameters:
      - name: max_date
        in: query
        type: string
        required: false
      - name: min_date
        in: query
        type: string
        required: false
      - name: station_id
        in: query
        type: integer
        required: false
      - name: page
        in: query
        type: integer
        required: false
        default: 1
      - name: per_page
        in: query
        type: integer
        required: false
        default: 50
    definitions:
        WeatherReport:
            type: object
            properties:
                id:
                    type: integer
                    format: int32
                    example: 1
                date:
                    type: string
                    format: date
                    example: '2010-01-01'
                max_temp:
                    type: number
                    format: float 
                    example: 37
                min_temp:
                    type: number
                    format: float
                    example: -2
                precipitation:
                    type: integer
                    format: int32
                    example: 129
                station_id:
                    type: integer
                    format: int32
                    example: 5
    responses:
        200:
            description: Daily maximum/minimum temperature and precipitation at each weather station, plus record ID number and creation date
            schema:
                type: 'array'
                items:
                    $ref: '#/definitions/WeatherReport'
    """
    return WeatherReport.get_pagination_JSON(
        request.args, max_date=dt.date.fromisoformat,
        min_date=dt.date.fromisoformat, station_id=int
    )


@bp.get("/weather/stats")
def get_weather_stats() -> dict[str, object]:
    """ Weather statistics endpoint
    ---
    responses:
        200:
            description: Weather summary statistics - total precipitation and average maximum and minimum temperature
    """
    # TODO Modularize; move code below into DBTable/WeatherReport method(s)

    # Build SQLAlchemy query to get overall yearly weather stats
    all_stats = dict()
    year = sa.extract("year", WeatherReport.date).label("year")
    query = db.select(
        WeatherReport.station_id, year,
        sa.func.avg(WeatherReport.min_temp).label("avg_min_temp_degC"),
        sa.func.avg(WeatherReport.max_temp).label("avg_max_temp_degC"),
        sa.func.sum(WeatherReport.precipitation).label("total_precip_cm")
    ).group_by(WeatherReport.station_id, year)

    # Only include the specific year(s) and weather station(s) requested
    which = dict()
    for filter_param in ("year", "station_id", "page", "per_page"):
        which[filter_param] = request.args.get(filter_param, type=int)
    if which.get("year") is not None:
        query.filter_by(year=which.get("year"))
    if which.get("station_id") is not None:
        query.filter_by(station_id=which.get("station_id"))

    # Execute and return query to get yearly weather stats for the station(s)
    all_stats = [row._asdict() for row in db.session.execute(query)]
    return jsonify(all_stats)


@bp.get("/weather/stations")
def get_weather_stations() -> Dict[str, Any]:
    """ Weather stations data endpoint
    ---
    parameters:
      - name: page
        in: query
        type: integer
        required: false
        default: 1
      - name: per_page
        in: query
        type: integer
        required: false
        default: 50
    definitions:
        WeatherStation:
            type: object
            properties:
                created:
                    type: string
                    format: date
                id:
                    type: integer
                    format: int32
                name:
                    type: string
                    format: string
                updated:
                    type: string
                    format: date
    responses:
        200:
            description: List the name, update/creation date, and ID number of each weather station
    """
    return WeatherStation.get_pagination_JSON(request.args)


@bp.get("/crop")
def get_crop_yield() -> Dict[str, Any]:
    """ Crop yield data endpoint
    ---
    parameters:
      - name: page
        in: query
        type: integer
        required: false
        default: 1
      - name: per_page
        in: query
        type: integer
        required: false
        default: 50
    definitions:
        CropYield:
            type: object
            properties:
                corn_bushels:
                    type: integer
                    format: int32
                    example: 123456
                created:
                    type: string
                    format: date
                    example: '2024-07-16T14:19:39.621049'
                id:
                    type: integer
                    format: int32
                    example: 6
                year:
                    type: integer
                    format: int32
                    example: 1990
    responses:
        200:
            description: Number of corn bushels per year, plus crop yield record ID and creation date
    """
    return CropYield.get_pagination_JSON(request.args)
