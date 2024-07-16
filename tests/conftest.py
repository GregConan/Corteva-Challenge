#!/usr/bin/env python3
# coding: utf-8

"""
Greg Conan: gregmconan@gmail.com
Created: 2024-07-14
Updated: 2024-07-15
"""
# PyPI imports
import pytest

# Local custom imports
from corteva_challenge import create_Flask_app


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
