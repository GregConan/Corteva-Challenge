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
    responses:
        200:
        description: JSON response containing daily weather data
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
        description: JSON response containing weather summary statistics
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
    responses:
        200:
        description: JSON response listing weather stations
    """
    return WeatherStation.get_pagination_JSON(request.args)


@bp.get("/crop")
def get_crop_yield() -> Dict[str, Any]:
    """ Crop yield data endpoint
    ---
    responses:
        200:
        description: JSON response containing yearly crop yield data
    """
    return CropYield.get_pagination_JSON(request.args)
