from datetime import datetime
import logging
from subprocess import run
import time

from mysql import connector
from mysql.connector.errors import (
    OperationalError, DatabaseError, InterfaceError
)
import pytest


DO_TEARDOWN = False
DB_HOST = 'localhost'
DB_USER = 'root'
DB_DB = 'edgar'


@pytest.fixture(scope='session')
def db():
    """Fixture for holding the database connector.
    Allows for recycling a single connection rather than re-creating for each
    test."""

    return connector.connect(
        host=DB_HOST,
        user=DB_USER,
        db=DB_DB
    )


def wait_for_db(poll_interval: int = 5, timeout: int = 300):
    """Checks every 5 seconds for a valid connection to the test database.
    Timeout occurs after 300 seconds.

    :param poll_interval: Interval to poll database at in seconds, defaults to
    5
    :type poll_interval: int, optional
    :param timeout: Timeout for database startup in seconds, defaults to 300
    :type timeout: int, optional
    :raises Exception: Timeout period was reached
    """

    # save the current datetime to calculate timeouts from
    start_time = datetime.now()

    # flag to hold if the database successfully started up
    success = False

    # wait for startup for no more than 300 seconds.
    while (datetime.now() - start_time).seconds < timeout:
        try:

            # attempt to create a connection
            connector.connect(
                host=DB_HOST,
                user=DB_USER,
                db=DB_DB
            )

            # if no exception was thrown, we can assume that the database has
            #     been fully restored and is ready for connections
            logging.info("Database restored.")
            success = True

            # break out of the while loop as we've verified our objective
            break
        except (InterfaceError, OperationalError):
            logging.info("Database isn't ready yet.")
            time.sleep(poll_interval)

    # the database connection was not verified within the timeout period
    if not success:
        raise Exception(
            f"Test database not reachable after {timeout} seconds."
        )


def create_test_db():
    """Utilizes the test docker-compose environment to start a database to run
    tests against. Requires that the docker-compose cli and docker environment
    be available.
    """

    # when this function is called we need to flag that we should compose down
    #     at the end of the tests
    global DO_TEARDOWN

    # command line arguments to call docker-compose up detached for the compose
    #     file in the tests folder
    compose_cli = [
        'docker-compose', '-f', './tests/test-docker-compose.yml', 'up', '-d'
    ]

    # execute the command line arguments
    startup_result = run(compose_cli)

    # verify that we got a 0 exit code from the command line execution
    #     validating successful startup
    startup_result.check_returncode()
    logging.info("Successful database startup.")

    # wait for the database to be up and ready for connections
    # the database is technically running at this point, but isn't ready for
    #     connections
    # we have to do this since the database does a restore after successful
    #     container startup
    # if we did not do this wait, connection exceptions would be thrown
    wait_for_db()

    # set the flag to do the container teardown at the end of the tests
    DO_TEARDOWN = True


def pytest_configure():
    """pytest pre-run script. Checks for locally running db and starts one if
    not found.
    """

    try:
        # attempt to create a database connection, we don't use this for
        #     anything other than verifying that it doesn't throw an exception
        connector.connect(
            host=DB_HOST,
            user=DB_USER,
            db=DB_DB
        )

    # no database responded to the connection attempt so assume that it needs
    #     to be created
    except DatabaseError:
        logging.info("Existing database not found. Creating test database.")
        create_test_db()

    # two exceptions that indicate that the database may exist, but can't be
    #     connected to yet
    # attempt to wait for database to be ready for connections
    # this would specifically be used when the database was started external
    #     to the `create_test_db` method shortly before the test run was
    #     attempted
    except (InterfaceError, OperationalError):
        logging.info("Database found but isn't ready yet.")
        wait_for_db()


def pytest_unconfigure():
    """pytest post-run script. Checks for the teardown flag to be set and runs
    docker-compose down to stop the test db that the pre-run script had
    started.
    """

    if DO_TEARDOWN:

        logging.info("Tearing down test configuration.")

        # command line arguments to call docker-compose up detached for the
        # compose file in the tests folder
        compose_cli = [
            'docker-compose', '-f', './tests/test-docker-compose.yml', 'down',
            '-v'
        ]

        # execute the command line arguments
        run(compose_cli)
