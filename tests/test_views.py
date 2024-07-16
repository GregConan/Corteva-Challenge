#!/usr/bin/env python3
# coding: utf-8

"""
Greg Conan: gregmconan@gmail.com
Created: 2024-07-14
Updated: 2024-07-14
"""
import pytest
from corteva_challenge.utilities import build_endpt_path


@pytest.mark.parametrize(("endpoint"), (
    ("/"), ("/api/weather/stats"), ("/api/weather"),
    (build_endpt_path("api", "weather", min_date="1998-01-01", per_page=30,
                      max_date="1999-01-21", station_id=3, page=2)),
    (build_endpt_path("api", "weather", "stations", per_page=5, page=2)),
    (build_endpt_path("api", "crop", per_page=5, page=2)),
))
def test_views(client, endpoint: str) -> None:
    """
    :param client
    """
    response = client.get(endpoint)
    assert response.status_code == 200
    print(f"{endpoint}: {response.text}")
