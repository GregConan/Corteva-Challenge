#!/usr/bin/env python3
# coding: utf-8

"""
Greg Conan: gregmconan@gmail.com
Created: 2024-07-12
Updated: 2024-07-12
"""

# PyPI imports
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import psycopg2
import sqlalchemy
from sqlalchemy import orm

# Local imports
from corteva_challenge.models import db


def create_Flask_app() -> Flask:  # (db: SQLAlchemy)
    # Create Flask app to attach to PostgreSQL DB
    app = Flask(__name__)
    app.config.from_pyfile("config.py")

    # Attach Flask app to PostgreSQLAlchemy DB object
    db.init_app(app)

    # Create DB Tables
    @app.cli.command("setup-db")
    def setup_db():
        db.create_all()

    return app
