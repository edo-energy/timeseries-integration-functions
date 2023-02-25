import datetime
import os
import pytest
from random import uniform
from statistics import mean

import pandas as pd
from pytest_mock import MockerFixture

from WeatherCollection import main, get_missing_dates


@pytest.fixture
def vc_response():
    def generate_hour(epoch):
        return {
            "datetimeEpoch": epoch,
            "temp": round(uniform(-10, 110), 1),
            "feelslike": round(uniform(-10, 110), 1),
            "humidity": round(uniform(0, 100), 1),
            "dew": round(uniform(-10, 110), 1),
            "precip": round(uniform(0, 1), 1),
            "precipprob": round(uniform(0, 100), 1),
            "windspeed": round(uniform(0, 80), 1),
            "winddir": round(uniform(0, 359.9), 1),
            "pressure": round(uniform(990, 1010), 1),
            "visibility": round(uniform(0, 100), 1),
            "cloudcover": round(uniform(0, 100), 1),
            "uvindex": round(uniform(0, 10), 1)
        }

    def generate_day(start_epoch):
        hours = []
        epoch = start_epoch

        while epoch <= start_epoch + 86400:
            hours.append(generate_hour(epoch))
            epoch += 3600

        return {
            "datetimeEpoch": start_epoch,
            "temp": mean([x["temp"] for x in hours]),
            "feelslike": mean([x["feelslike"] for x in hours]),
            "humidity": mean([x["humidity"] for x in hours]),
            "dew": mean([x["dew"] for x in hours]),
            "precip": mean([x["precip"] for x in hours]),
            "precipprob": mean([x["precipprob"] for x in hours]),
            "windspeed": mean([x["windspeed"] for x in hours]),
            "winddir": mean([x["winddir"] for x in hours]),
            "pressure": mean([x["pressure"] for x in hours]),
            "visibility": mean([x["visibility"] for x in hours]),
            "cloudcover": mean([x["cloudcover"] for x in hours]),
            "uvindex": mean([x["uvindex"] for x in hours]),
            "hours": hours
        }

    start_epoch = int(
        datetime.datetime.now()
        .replace(
            hour=0,
            minute=0,
            second=0,
            microsecond=0
        )
        .timestamp()
    )

    return {
        "queryCost": 0,
        "latitude": 37.77999,
        "longitude": -122.419998,
        "resolvedAddress": "37.77999,-122.419998",
        "address": "37.77999,-122.419998",
        "timezone": "America/Los_Angeles",
        "tzoffset": -8.0,
        "days": [generate_day(start_epoch)]
    }


@pytest.fixture(scope="module", autouse=True)
def db_fixture(db, module_mocker):
    module_mocker.patch("WeatherCollection.getConnection", return_value=db)


def test_get_missing_dates(db):
    results = get_missing_dates(db, 88242)

    assert results.size > 0


def test_WeatherCollection(mocker: MockerFixture, vc_response):
    # Arrange
    missing_dates = pd.DataFrame(
        [(None, None, datetime.datetime(2000, 1, 1))],
        columns=['SiteID', 'PointID', 'ts']
    )
    mocker.patch(
        'WeatherCollection.get_missing_dates',
        return_value=missing_dates
    )

    mock_json = mocker.Mock()
    mock_json.return_value = vc_response

    mock_api = mocker.patch('WeatherCollection.r.get')
    mock_api.return_value.json = mock_json

    mocker.patch.dict(os.environ, {"VC_API_KEY": "test"})

    # Act
    main(None)

    # Assert
    assert mock_api.call_count > 0
