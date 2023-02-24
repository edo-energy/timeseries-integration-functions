import datetime
import os
import pytest

import pandas as pd
from pytest_mock import MockerFixture

from WeatherCollection import main, get_missing_dates

visual_crossing_response = {
    "queryCost": 0,
    "latitude": 37.77999,
    "longitude": -122.419998,
    "resolvedAddress": "37.77999,-122.419998",
    "address": "37.77999,-122.419998",
    "timezone": "America/Los_Angeles",
    "tzoffset": -8.0,
    "days": [
        {
            "datetimeEpoch": 1672560000,
            "temp": 53.4,
            "feelslike": 52.6,
            "dew": 41.0,
            "humidity": 64.6,
            "precip": 0.67,
            "precipprob": 100.0,
            "windspeed": 13.2,
            "winddir": 306.7,
            "pressure": 1011.3,
            "cloudcover": 28.5,
            "visibility": 9.9,
            "uvindex": 5.0,
            "hours": [
                {
                    "datetimeEpoch": 1672560000,
                    "temp": 52.5,
                    "feelslike": 52.5,
                    "humidity": 82.03,
                    "dew": 47.2,
                    "precip": 0.0,
                    "precipprob": 0.0,
                    "windspeed": 8.1,
                    "winddir": 310.0,
                    "pressure": 1008.6,
                    "visibility": 9.9,
                    "cloudcover": 90.9,
                    "uvindex": 0.0
                },
                {
                    "datetimeEpoch": 1672563600,
                    "temp": 50.7,
                    "feelslike": 50.7,
                    "humidity": 86.08,
                    "dew": 46.7,
                    "precip": 0.0,
                    "precipprob": 0.0,
                    "windspeed": 7.2,
                    "winddir": 297.0,
                    "pressure": 1008.3,
                    "visibility": 9.9,
                    "cloudcover": 36.8,
                    "uvindex": 0.0
                },
                {
                    "datetimeEpoch": 1672567200,
                    "temp": 50.1,
                    "feelslike": 50.1,
                    "humidity": 84.95,
                    "dew": 45.8,
                    "precip": 0.0,
                    "precipprob": 0.0,
                    "windspeed": 5.9,
                    "winddir": 300.0,
                    "pressure": 1008.1,
                    "visibility": 8.9,
                    "cloudcover": 23.0,
                    "uvindex": 0.0
                },
                {
                    "datetimeEpoch": 1672570800,
                    "temp": 50.0,
                    "feelslike": 45.6,
                    "humidity": 84.94,
                    "dew": 45.6,
                    "precip": 0.0,
                    "precipprob": 0.0,
                    "windspeed": 11.2,
                    "winddir": 280.0,
                    "pressure": 1008.7,
                    "visibility": 9.9,
                    "cloudcover": 0.0,
                    "uvindex": 0.0
                },
                {
                    "datetimeEpoch": 1672574400,
                    "temp": 49.9,
                    "feelslike": 45.2,
                    "humidity": 81.95,
                    "dew": 44.6,
                    "precip": 0.67,
                    "precipprob": 100.0,
                    "windspeed": 12.2,
                    "winddir": 308.0,
                    "pressure": 1009.3,
                    "visibility": 9.9,
                    "cloudcover": 0.0,
                    "uvindex": 0.0
                },
                {
                    "datetimeEpoch": 1672578000,
                    "temp": 48.8,
                    "feelslike": 44.8,
                    "humidity": 82.37,
                    "dew": 43.6,
                    "precip": 0.0,
                    "precipprob": 0.0,
                    "windspeed": 9.2,
                    "winddir": 304.0,
                    "pressure": 1009.8,
                    "visibility": 9.9,
                    "cloudcover": 0.0,
                    "uvindex": 0.0
                },
                {
                    "datetimeEpoch": 1672581600,
                    "temp": 48.1,
                    "feelslike": 44.3,
                    "humidity": 81.04,
                    "dew": 42.5,
                    "precip": 0.0,
                    "precipprob": 0.0,
                    "windspeed": 8.3,
                    "winddir": 322.0,
                    "pressure": 1010.4,
                    "visibility": 9.9,
                    "cloudcover": 0.0,
                    "uvindex": 0.0
                },
                {
                    "datetimeEpoch": 1672585200,
                    "temp": 47.1,
                    "feelslike": 46.1,
                    "humidity": 82.13,
                    "dew": 42.0,
                    "precip": 0.0,
                    "precipprob": 0.0,
                    "windspeed": 3.3,
                    "winddir": 319.0,
                    "pressure": 1011.0,
                    "visibility": 9.9,
                    "cloudcover": 11.4,
                    "uvindex": 0.0
                },
                {
                    "datetimeEpoch": 1672588800,
                    "temp": 48.1,
                    "feelslike": 47.2,
                    "humidity": 80.34,
                    "dew": 42.3,
                    "precip": 0.0,
                    "precipprob": 0.0,
                    "windspeed": 3.4,
                    "winddir": 333.0,
                    "pressure": 1011.7,
                    "visibility": 9.9,
                    "cloudcover": 11.4,
                    "uvindex": 0.0
                },
                {
                    "datetimeEpoch": 1672592400,
                    "temp": 51.9,
                    "feelslike": 51.9,
                    "humidity": 72.31,
                    "dew": 43.3,
                    "precip": 0.0,
                    "precipprob": 0.0,
                    "windspeed": 6.2,
                    "winddir": 310.0,
                    "pressure": 1012.2,
                    "visibility": 9.9,
                    "cloudcover": 11.4,
                    "uvindex": 3.0
                },
                {
                    "datetimeEpoch": 1672596000,
                    "temp": 53.6,
                    "feelslike": 53.6,
                    "humidity": 69.01,
                    "dew": 43.6,
                    "precip": 0.0,
                    "precipprob": 0.0,
                    "windspeed": 9.8,
                    "winddir": 280.0,
                    "pressure": 1012.9,
                    "visibility": 9.9,
                    "cloudcover": 11.4,
                    "uvindex": 4.0
                },
                {
                    "datetimeEpoch": 1672599600,
                    "temp": 55.9,
                    "feelslike": 55.9,
                    "humidity": 62.47,
                    "dew": 43.3,
                    "precip": 0.0,
                    "precipprob": 0.0,
                    "windspeed": 9.2,
                    "winddir": 293.0,
                    "pressure": 1012.6,
                    "visibility": 9.9,
                    "cloudcover": 0.0,
                    "uvindex": 5.0
                },
                {
                    "datetimeEpoch": 1672603200,
                    "temp": 57.3,
                    "feelslike": 57.3,
                    "humidity": 56.5,
                    "dew": 42.0,
                    "precip": 0.0,
                    "precipprob": 0.0,
                    "windspeed": 4.7,
                    "winddir": 338.0,
                    "pressure": 1011.9,
                    "visibility": 9.9,
                    "cloudcover": 11.5,
                    "uvindex": 5.0
                },
                {
                    "datetimeEpoch": 1672606800,
                    "temp": 57.7,
                    "feelslike": 57.7,
                    "humidity": 55.74,
                    "dew": 42.0,
                    "precip": 0.0,
                    "precipprob": 0.0,
                    "windspeed": 7.2,
                    "winddir": 348.0,
                    "pressure": 1011.1,
                    "visibility": 9.9,
                    "cloudcover": 11.5,
                    "uvindex": 5.0
                },
                {
                    "datetimeEpoch": 1672610400,
                    "temp": 61.1,
                    "feelslike": 61.1,
                    "humidity": 45.26,
                    "dew": 39.7,
                    "precip": 0.0,
                    "precipprob": 0.0,
                    "windspeed": 5.7,
                    "winddir": 326.0,
                    "pressure": 1010.4,
                    "visibility": 9.9,
                    "cloudcover": 23.0,
                    "uvindex": 5.0
                },
                {
                    "datetimeEpoch": 1672614000,
                    "temp": 60.9,
                    "feelslike": 60.9,
                    "humidity": 50.75,
                    "dew": 42.5,
                    "precip": 0.0,
                    "precipprob": 0.0,
                    "windspeed": 12.5,
                    "winddir": 290.0,
                    "pressure": 1011.0,
                    "visibility": 9.9,
                    "cloudcover": 32.2,
                    "uvindex": 3.0
                },
                {
                    "datetimeEpoch": 1672617600,
                    "temp": 59.7,
                    "feelslike": 59.7,
                    "humidity": 46.01,
                    "dew": 38.9,
                    "precip": 0.0,
                    "precipprob": 0.0,
                    "windspeed": 13.2,
                    "winddir": 300.0,
                    "pressure": 1011.7,
                    "visibility": 9.9,
                    "cloudcover": 32.4,
                    "uvindex": 1.0
                },
                {
                    "datetimeEpoch": 1672621200,
                    "temp": 57.4,
                    "feelslike": 57.4,
                    "humidity": 48.96,
                    "dew": 38.4,
                    "precip": 0.0,
                    "precipprob": 0.0,
                    "windspeed": 12.7,
                    "winddir": 317.0,
                    "pressure": 1012.2,
                    "visibility": 9.9,
                    "cloudcover": 57.8,
                    "uvindex": 0.0
                },
                {
                    "datetimeEpoch": 1672624800,
                    "temp": 55.5,
                    "feelslike": 55.5,
                    "humidity": 48.86,
                    "dew": 36.6,
                    "precip": 0.0,
                    "precipprob": 0.0,
                    "windspeed": 8.0,
                    "winddir": 309.0,
                    "pressure": 1013.1,
                    "visibility": 9.9,
                    "cloudcover": 32.2,
                    "uvindex": 0.0
                },
                {
                    "datetimeEpoch": 1672628400,
                    "temp": 55.3,
                    "feelslike": 55.3,
                    "humidity": 45.76,
                    "dew": 34.7,
                    "precip": 0.0,
                    "precipprob": 0.0,
                    "windspeed": 7.4,
                    "winddir": 318.0,
                    "pressure": 1013.2,
                    "visibility": 9.9,
                    "cloudcover": 57.8,
                    "uvindex": 0.0
                },
                {
                    "datetimeEpoch": 1672632000,
                    "temp": 53.4,
                    "feelslike": 53.4,
                    "humidity": 49.23,
                    "dew": 34.8,
                    "precip": 0.0,
                    "precipprob": 0.0,
                    "windspeed": 6.3,
                    "winddir": 303.0,
                    "pressure": 1013.4,
                    "visibility": 9.9,
                    "cloudcover": 23.0,
                    "uvindex": 0.0
                },
                {
                    "datetimeEpoch": 1672635600,
                    "temp": 52.4,
                    "feelslike": 52.4,
                    "humidity": 52.29,
                    "dew": 35.4,
                    "precip": 0.0,
                    "precipprob": 0.0,
                    "windspeed": 7.6,
                    "winddir": 303.0,
                    "pressure": 1013.7,
                    "visibility": 9.9,
                    "cloudcover": 57.8,
                    "uvindex": 0.0
                },
                {
                    "datetimeEpoch": 1672639200,
                    "temp": 52.5,
                    "feelslike": 52.5,
                    "humidity": 47.03,
                    "dew": 32.9,
                    "precip": 0.0,
                    "precipprob": 0.0,
                    "windspeed": 5.8,
                    "winddir": 310.0,
                    "pressure": 1013.6,
                    "visibility": 9.9,
                    "cloudcover": 74.1,
                    "uvindex": 0.0
                },
                {
                    "datetimeEpoch": 1672642800,
                    "temp": 50.9,
                    "feelslike": 50.9,
                    "humidity": 54.75,
                    "dew": 35.2,
                    "precip": 0.0,
                    "precipprob": 0.0,
                    "windspeed": 2.9,
                    "winddir": 330.0,
                    "pressure": 1013.3,
                    "visibility": 9.9,
                    "cloudcover": 74.1,
                    "uvindex": 0.0
                }
            ]
        }
    ]
}


@pytest.fixture(scope="module", autouse=True)
def db_fixture(db, module_mocker):
    module_mocker.patch("WeatherCollection.getConnection", return_value=db)


def test_get_missing_dates(db):
    results = get_missing_dates(db, 88242)

    assert results.size > 0


def test_WeatherCollection(mocker: MockerFixture):
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
    mock_json.return_value = visual_crossing_response

    mock_api = mocker.patch('WeatherCollection.r.get')
    mock_api.return_value.json = mock_json

    mocker.patch.dict(os.environ, {"VC_API_KEY": "test"})

    # Act
    main()

    # Assert
    assert mock_api.call_count > 0
