#!/usr/bin/env python3
# coding: utf-8

"""
Greg Conan: gregmconan@gmail.com
Created: 2024-07-12
Updated: 2024-07-15
"""
# Import standard libraries
import datetime as dt
from typing import Any, Dict

# PyPI imports
from flask import Blueprint, jsonify, request
import sqlalchemy as sa

# Local custom imports
from corteva_challenge.models import CropYield, db, WeatherReport, WeatherStation
from corteva_challenge.models import db


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
        default: all
      - name: min_date
        in: query
        type: string
        required: false
        default: all
      - name: station_id
        in: query
        type: integer
        required: false
        default: all
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
                date:
                    type: string
                    format: date
                max_temp:
                    type: number
                    format: float 
                min_temp:
                    type: number
                    format: float
                precipitation:
                    type: integer
                    format: int32
                station_id:
                    type: integer
                    format: int32
    responses:
        200:
            description: Daily maximum/minimum temperature and precipitation at each weather station, plus record ID number and creation date
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
    all_stats = dict()
    for col_name in ("max_temp", "min_temp"):
        all_stats[f"avg_{col_name}_degC"] = WeatherReport.run_math_query_on(
            col_name, sa.func.avg)
    all_stats["total_precip_cm"] = WeatherReport.run_math_query_on(
        "precipitation", sa.func.sum)
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
                created:
                    type: string
                    format: date
                id:
                    type: integer
                    format: int32
                year:
                    type: string
                    format: date
    responses:
        200:
            description: Number of corn bushels per year, plus crop yield record ID and creation date
    """
    return CropYield.get_pagination_JSON(request.args)
