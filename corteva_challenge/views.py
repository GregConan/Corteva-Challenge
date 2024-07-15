#!/usr/bin/env python3
# coding: utf-8

"""
Greg Conan: gregmconan@gmail.com
Created: 2024-07-12
Updated: 2024-07-13
"""
# Import standard libraries
import datetime as dt

# PyPI imports
from flask import Blueprint, jsonify, request
import psycopg2
import sqlalchemy as sa
from sqlalchemy import orm
from sqlalchemy.dialects.postgresql import insert

# Local custom imports
from corteva_challenge.models import CropYield, db, WeatherReport, WeatherStation
from corteva_challenge.models import db


bp = Blueprint("weather", __name__, url_prefix="/api")


@bp.get("/weather")
def get_weather() -> dict[str, object]:
    params = dict(
        # , type=dt.date),
        max_date=request.args.get("max_date", type=dt.date.fromisoformat),
        # , type=dt.date),
        min_date=request.args.get("min_date", type=dt.date.fromisoformat),
        station_id=request.args.get("station_id", type=int),
        page=request.args.get("page", type=int, default=1),
        per_page=request.args.get("per_page", type=int, default=50)
    )
    result_page = WeatherReport.run_page_query(**params)
    return jsonify(page=result_page.page,
                   items=[x.to_dict() for x in result_page.items],
                   total=result_page.total, next=result_page.next_num)


@bp.get("/weather/stats")
def get_weather_stats() -> dict[str, object]:

    # weather = db.session.execute(db.select(WeatherReport).order_by(WeatherReport.id))
    # weather = db.session.execute(WeatherReport.get_avg_max()).scalar()
    # weather = db.session.execute(sa.func.count(WeatherReport.max_temp))

    # sa.func.sum(WeatherReport.max_temp)

    # return render_template("user/list.html", users=users)
    # db.session.query().select_from
    # db.session.get()
    all_stats = dict()
    for col_name in ("max_temp", "min_temp"):
        all_stats[f"avg_{col_name}_degC"] = WeatherReport.run_math_query_on(
            col_name, sa.func.avg)
    all_stats["total_precip_cm"] = WeatherReport.run_math_query_on(
        "precipitation", sa.func.sum)
    return jsonify(all_stats)


@bp.get("/weather/stations")
def get_weather_stations() -> dict[str, object]:
    pass
