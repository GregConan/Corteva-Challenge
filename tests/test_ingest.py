#!/usr/bin/env python3
# coding: utf-8


# Standard imports
import os

# PyPI imports
import dask.dataframe as dd
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import psycopg2
import sqlalchemy
from sqlalchemy import orm

# Local custom imports
from corteva_challenge.ingest import ingest


def test_ingest(app: Flask):

    ingest(os.getenv("GITHUB_TOKEN"))
