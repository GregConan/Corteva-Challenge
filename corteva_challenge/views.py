#!/usr/bin/env python3
# coding: utf-8

"""
Greg Conan: gregmconan@gmail.com
Created: 2024-07-12
Updated: 2024-07-13
"""
from flask import Blueprint
from flask import request


bp = Blueprint("weather", __name__, url_prefix="/api")


@bp.get("/weather")
def result(id: str) -> dict[str, object]:
    ready = result.ready()
    return {
        "ready": ready,
        "successful": result.successful() if ready else None,
        "value": result.get() if ready else result.result,
    }


@bp.get("/weather/stats")
def result(id: str) -> dict[str, object]:
    ready = result.ready()
    return {
        "ready": ready,
        "successful": result.successful() if ready else None,
        "value": result.get() if ready else result.result,
    }


@bp.get("/weather/stations")
def result(id: str) -> dict[str, object]:
    ready = result.ready()
    return {
        "ready": ready,
        "successful": result.successful() if ready else None,
        "value": result.get() if ready else result.result,
    }
