
#!/usr/bin/env python3
# coding: utf-8

# Import standard libraries
import os

# PyPI imports
import dask.dataframe as dd
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import psycopg2
import sqlalchemy
from sqlalchemy import orm

# Local custom imports


def test_setup(app: Flask):
    runner = app.test_cli_runner()
    result = runner.invoke(args=["setup-db"])
    print(result.output)
