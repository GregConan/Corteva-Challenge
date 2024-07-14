#!/usr/bin/env python3
# coding: utf-8

# Standard imports
import os

# PyPI imports
from dotenv import load_dotenv
import pytest

# Local custom imports
from corteva_challenge import create_Flask_app

# env_fpath = os.path.join(os.path.expanduser("~"), ".tokens", ".corteva.env")
env_fpath = "/home/gconan/.tokens/.corteva.env"
assert os.path.exists(env_fpath)
loaded = load_dotenv(env_fpath)
assert loaded


@pytest.fixture()
def app():
    """ Fixture to create flask app from factory
    and initiate app context
    """
    app = create_Flask_app()
    app.config.update({
        "TESTING": True,
    })

    with app.app_context():
        yield app


@pytest.fixture()
def client(app):
    """ Fixture to create test client for view tests
    """
    return app.test_client()
