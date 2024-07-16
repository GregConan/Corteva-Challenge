#!/usr/bin/env python3
# coding: utf-8

"""
Greg Conan: gregmconan@gmail.com
Created: 2024-07-14
Updated: 2024-07-15
"""
# PyPI imports
from flask import Flask

# Local custom imports
from corteva_challenge import db
# from corteva_challenge.utilities import log  # TODO replace print with log


def test_setup(app: Flask) -> None:
    # GIVEN: Setup test
    db.drop_all()

    # WHEN: Run test
    runner = app.test_cli_runner()
    result = runner.invoke(args=["setup-db"])

    # THEN: Check output
    print(result.output)
