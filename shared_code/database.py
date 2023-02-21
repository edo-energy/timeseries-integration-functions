from mysql import connector
import os
import logging


def getConnection() -> connector.MySQLConnection:

    logging.info("Creating database connector.")

    conn = connector.connect(
        host=os.environ['db_HOST'],
        user=os.environ['db_USERNAME'],
        password=os.environ['db_PASSWORD'],
        db=os.environ['db_DATABASE']
    )

    return conn
