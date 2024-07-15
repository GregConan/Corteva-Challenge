#!/usr/bin/env python3
# coding: utf-8

"""
Greg Conan: gregmconan@gmail.com
Created: 2024-07-14
Updated: 2024-07-14
"""
import pytest


@pytest.mark.parametrize(("endpoint"), (
    ("/"), ("/api/weather/stats"), ("/api/weather"),
    ("/api/weather?min_date=1999-01-01&max_date=1999-01-21&station_id=3&page=2&per_page=30"),
))
def test_views(client, endpoint: str):
    response = client.get(endpoint)
    assert response.status_code == 200
    print(f"{endpoint}: {response.text}")
