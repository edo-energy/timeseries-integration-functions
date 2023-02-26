import logging
import os
from urllib.parse import urljoin

from mysql.connector import MySQLConnection
import pandas as pd
import requests as r

from shared_code.database import getConnection

select_dates_sql = """
SELECT
  SiteID,
  PointID,
  TIMESTAMP(date_value) AS 'ts'
FROM
  dim_date
LEFT JOIN
  tr ON (
    tr.datevalue = dim_date.date_value
    AND SiteID = 140
    AND PointID = %s
  )
WHERE
  dim_date.date_value >= DATE_SUB(NOW(), INTERVAL %s YEAR)
  AND dim_date.date_value < DATE(NOW())
  AND tr.datevalue IS NULL
"""


def get_missing_dates(conn: MySQLConnection, pointid: int) -> pd.DataFrame:
    lb_years = 0.5

    with conn.cursor() as cur:
        cur.execute(select_dates_sql, (pointid, lb_years))

        dates_result = cur.fetchall()

        df_dates = pd.DataFrame(
            dates_result,
            columns=['SiteID', 'PointID', 'ts']
        )

    return df_dates


def main(params: tuple):
    error = False

    # no more than 1000 calls per run
    cur_calls = 0

    # api key for visual crossing requests
    vc_api_key = os.environ.get("VC_API_KEY")

    # ensure api key is defined
    if vc_api_key is None:
        raise ValueError("VC_API_KEY is not defined.")

    # EDGAR database connection
    db = getConnection()

    select_stations_sql = """SELECT ID,Latitude,Longitude,Link,PointID
    FROM regressionweatherstations
    WHERE Enabled = 1"""

    select_point_sql = """SELECT ID
    FROM point
    WHERE SiteID = %s
    AND PointName = %s"""

    insert_point_sql = """INSERT INTO point
    (SiteID, PointName, PointClassID)
    Values(%s,%s,%s)"""

    insert_trends_sql = """INSERT INTO tr
    (SiteID, PointID, datevalue, timevalue, rdValue, NumericValue)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      rdValue=VALUES(rdValue), NumericValue=VALUES(NumericValue)"""

    # get the currently enabled regression weather stations
    with db.cursor() as cur:
        cur.execute(select_stations_sql)

        df_stations = list(cur.fetchall())

    # all the weather points are held in site 140
    siteid = 140

    # key to point class match
    characteristics = {
        "precip":       10143,
        "precipprob":   10144,
        "temp":         13,
        "feelslike":    10145,
        "dew":          182,
        "humidity":     97,
        "pressure":     692,
        "windspeed":    694,
        "winddir":      695,
        "cloudcover":   696,
        "uvindex":      10146,
        "visibility":   10147
    }

    # List of parameters to request from weather api
    apiParameters = list(characteristics.keys())
    apiParameters.append('datetimeEpoch')

    # iterate through each of the stations and fill any required data
    for station in df_stations:

        logging.info(station[3])

        # dict to hold point ids
        point_ids = {}

        # check and create any new points
        for key, value in characteristics.items():
            # init key
            point_ids[key] = {}
            point_name = station[3] + '-' + key

            with db.cursor() as cur:
                cur.execute(select_point_sql, (siteid, point_name))

                point_result = cur.fetchone()

                # check to see if a new point needs to be created
                if point_result is None:
                    cur.execute(insert_point_sql, (siteid, point_name, value))

                    cur.execute(select_point_sql, (siteid, point_name))

                    point_result = cur.fetchone()

                    logging.info(
                        f"Created point {point_result[0]} for {point_name}."
                    )

            point_ids[key]['ID'] = point_result[0]
            point_ids[key]['Values'] = []

            logging.info(f"Found point {point_result[0]} for {point_name}.")

        # dates that need to get data for current point
        df_dates = get_missing_dates(db, station[4])

        # base url to make the requests to
        base_url = "https://weather.visualcrossing.com/"\
            "VisualCrossingWebServices/rest/services/timeline/"

        if len(df_dates) == 0:
            logging.info(f"No new data needed for {station[3]}")
            continue

        for _, missing in df_dates.iterrows():
            logging.info(missing['ts'])

            # make the api request to get the required weather data
            params = {
                "key": vc_api_key,
                "include": "hours",
                "elements": ','.join(apiParameters)
            }
            response = r.get(
                urljoin(
                    base_url,
                    f"{station[1]},{station[2]}/" +
                    missing['ts'].strftime("%Y-%m-%d")
                ),
                params
            )

            cur_calls += 1

            if response.status_code != 200:
                logging.error(
                    f"API Returned: {response.status_code} - {response.text}"
                )
                logging.warning("Not all missing days were collected.")
                error = True
                break

            # parse the json response data
            json_response = response.json()
            weather_data = json_response['days'][0]['hours']

            # read the response into a dataframe
            df = pd.DataFrame.from_records(weather_data)

            # read the timezone
            timezone = json_response['timezone']

            # convert the seconds time into a datetime
            df['datetimeEpoch'] = pd.to_datetime(df['datetimeEpoch'], unit='s')
            # set the index as the time column
            df = df.set_index(pd.DatetimeIndex(df['datetimeEpoch']))

            # set the datetime index as utc relative and then convert to the
            # local timezone
            df = df.tz_localize(tz='UTC')\
                .tz_convert(tz=timezone)\
                .tz_localize(tz=None)

            # remove duplicates due to daylight savings
            df = df.loc[~df.index.duplicated(keep='first')]

            # build each of the series
            for key, value in point_ids.items():
                point_ids[key]['Values'] +=\
                    df[key].reset_index().values.tolist()

        # insert the data into EDGAR
        for key, value in point_ids.items():
            with db.cursor() as cur:
                params = []
                for trend in value['Values']:
                    params.append(
                        (
                            siteid,
                            value['ID'],
                            trend[0].strftime('%Y-%m-%d'),
                            trend[0].strftime('%H:%M:%S'),
                            str(trend[1]),
                            float(trend[1])
                        )
                    )

                if len(params) > 0:
                    logging.info("Inserting data.")
                    cur.executemany(insert_trends_sql, params)
                else:
                    logging.warning("No data to insert.")

        # indicate that this station was looked at by updating the LastRun
        # column
        with db.cursor() as cur:
            cur.execute(
                """
                UPDATE
                regressionweatherstations
                SET
                LastRun = NOW()
                WHERE
                ID = %s
                """,
                (station[0],)
            )

        # commit after each station update
        db.commit()

    # update degree days
    with db.cursor() as cur:
        # create a temporary table that contains the degree day calculations
        cur.execute(
            """
            CREATE TEMPORARY TABLE
              degreedays
            SELECT
              point.SiteID,
              regressionweatherdeppoints.PointID,
              p1.DateValue,
              '00:00:00' AS 'timevalue',
              IF (
                point.PointClassID = 622,
                IF (
                    AVG(tr.NumericValue)
                    -GET_NUMERIC(pointmetadata.MetadataValue) < 0,
                    0,
                    AVG(tr.NumericValue)
                    -GET_NUMERIC(pointmetadata.MetadataValue)
                ),
                IF(
                    GET_NUMERIC(pointmetadata.MetadataValue)
                    -AVG(tr.NumericValue) < 0,
                    0,
                    GET_NUMERIC(pointmetadata.MetadataValue)
                    -AVG(tr.NumericValue)
                )
              ) AS 'NumericValue'
            FROM
              regressionweatherdeppoints
            LEFT JOIN
              regressionweatherstations ON (
                regressionweatherdeppoints.WeatherStationID =
                regressionweatherstations.ID
              )
            LEFT JOIN
              pointdaily p1 ON (
                p1.PointID =
                regressionweatherstations.PointID
              )
            LEFT JOIN
              pointdaily p2 ON (
                p2.PointID =
                regressionweatherdeppoints.PointID
                AND p1.DateValue = p2.DateValue
              )
            JOIN
              point ON (
                point.ID =
                regressionweatherdeppoints.PointID
              )
            LEFT JOIN
              pointmetadata ON (
                pointmetadata.point_id =
                point.ID
                AND pointmetadata.Metadata_id=624
              )
            LEFT JOIN
              tr ON (
                tr.SiteID = 140 AND tr.PointID =
                regressionweatherstations.PointID
                AND tr.datevalue = p1.Datevalue
              )
            WHERE
              regressionweatherstations.Enabled = 1
              AND (p2.NewRecords IS NULL OR p2.NewRecords < p1.NewRecords)
            GROUP BY
              PointID, DateValue
            """
        )

        # insert the degree day records into their own points
        cur.execute(
            """
            INSERT INTO
              tr (SiteID,PointID,datevalue,timevalue,NumericValue)
            (SELECT * FROM degreedays)
            ON DUPLICATE KEY UPDATE
              NumericValue=VALUES(NumericValue)
            """
        )

        # drop the temporary table
        cur.execute("""DROP TEMPORARY TABLE degreedays""")

        # update site aliased points
        # insert any new points that need to be created
        cur.execute(
            """
            INSERT INTO
              point (SiteID,PointName, BldgID,PointClassID, TypeID)
            (
                SELECT
                  building.SiteID,
                  CONCAT(
                    building.BuildingName,
                    ' ',
                    SUBSTRING(weather.PointName,6)
                  ) AS 'PointName',
                  regressionweatherstationalias.BuildingID,
                  weather.PointClassID,
                  7
                FROM
                  regressionweatherstationalias
                JOIN
                  building ON (
                    regressionweatherstationalias.BuildingID =
                    building.ID
                  )
                JOIN
                  regressionweatherstations ON (
                    regressionweatherstationalias.WeatherStationID =
                    regressionweatherstations.ID
                  )
                JOIN
                  point weather ON (
                    regressionweatherstations.Link =
                    LEFT(weather.PointName,4)
                    AND weather.SiteID=140
                  )
                LEFT JOIN
                  point ON (
                    building.ID =
                    point.BldgID
                    AND weather.PointClassID =
                    point.PointClassID AND point.Hidden=0
                  )
                WHERE
                  point.ID IS NULL
            )""")

        # create a temporary table to hold the data to insert into the aliased
        # points
        cur.execute(
            """
            CREATE TEMPORARY TABLE
              weatheralias
            SELECT building.SiteID,
                    point.ID,
                    tr.datevalue,
                    tr.timevalue,
                    tr.NumericValue
            FROM
              regressionweatherstationalias
            JOIN
              building ON (
                regressionweatherstationalias.BuildingID =
                building.ID
              )
            JOIN
              regressionweatherstations ON (
                regressionweatherstationalias.WeatherStationID =
                regressionweatherstations.ID
              )
            JOIN
              point weather ON (
                regressionweatherstations.Link =
                LEFT(weather.PointName,4)
                AND weather.SiteID=140
              )
            JOIN
              pointdaily weatherdaily ON (weather.ID = weatherdaily.PointID)
            JOIN
              tr ON (
                weather.SiteID = tr.SiteID
                AND weather.ID = tr.PointID
                AND weatherdaily.DateValue = tr.DateValue
              )
            LEFT JOIN
              point ON (
                building.ID = point.BldgID
                AND weather.PointClassID = point.PointClassID
                AND point.Hidden=0
              )
            LEFT JOIN
              pointdaily ON (
                point.ID = pointdaily.PointID
                AND weatherdaily.DateValue = pointdaily.DateValue
              )
            WHERE (
                pointdaily.DateValue IS NULL
                OR pointdaily.NewRecords < weatherdaily.NewRecords
            ) AND point.TypeID=7
            """
        )

        # insert then weather data into the alias points
        cur.execute(
            """
            INSERT INTO
              tr (SiteID,PointID,datevalue,timevalue,NumericValue)
            (SELECT * FROM weatheralias)
            ON DUPLICATE KEY UPDATE NumericValue = VALUES(NumericValue)
            """
        )

        # drop the temporary table
        cur.execute("""DROP TEMPORARY TABLE weatheralias""")

    db.commit()

    if error:
        raise Exception(
          "At least one error was encountered that may have resulted in missed"
          " or missing data."
        )

    return "Success."
