#!/usr/bin/env python3
# coding: utf-8

"""
Greg Conan: gregmconan@gmail.com
Created: 2024-07-12
Updated: 2024-07-14
"""
# PyPI imports
from flasgger import Swagger
from flask import Flask
from flask import jsonify

# Local custom imports
from corteva_challenge.models import db
from corteva_challenge.ingest import ingest
from corteva_challenge.views import bp


def create_Flask_app() -> Flask:
    # Create Flask app to attach to PostgreSQL DB
    app = Flask(__name__)
    app.config.from_pyfile("config.py")

    # Attach Flask app to PostgreSQLAlchemy DB object
    db.init_app(app)

    swagger = Swagger(app)

    @app.get("/")
    def index():
        """ Health check endpoint
        ---
        responses:
          200:
            description: A message confirming API is running
            examples:
              message: API is running
        """
        return jsonify(message="API is running.")

    # Create DB Tables
    @app.cli.command("setup-db")
    def setup_db():
        db.create_all()

    # Load weather data
    @app.cli.command("load-data")
    def load_data():
        ingest(app.config["GITHUB_TOKEN"])

    app.register_blueprint(bp)

    return app
