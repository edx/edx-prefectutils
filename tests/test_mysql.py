import mock
import pytest
from prefect.core import Flow
from prefect.engine import signals
from pytest_mock import mocker  # noqa: F401

from edx_prefectutils import mysql as utils_mysql


@pytest.fixture
def mock_mysql_connection(mocker):  # noqa: F811
    # Mock the Snowflake connection and cursor.
    mocker.patch.object(utils_mysql, 'create_mysql_connection')
    mock_cursor = mocker.Mock()
    mock_connection = mocker.Mock()
    mock_connection.cursor.return_value = mock_cursor
    utils_mysql.create_mysql_connection.return_value = mock_connection
    return mock_connection


def test_load_s3_data_to_mysql_no_overwrite_existing_data(mock_mysql_connection):
    mock_cursor = mock_mysql_connection.cursor()
    mock_fetchone = mock.Mock()
    mock_cursor.fetchone = mock_fetchone

    task = utils_mysql.load_s3_data_to_mysql
    with pytest.raises(
        signals.SKIP,
        match="Skipping task as data already exists in the dest. table and no overwrite was provided."
    ):
        task.run(
            aurora_credentials={},
            database="test_database",
            table="test_table",
            table_columns=[('id', 'int'), ('course_id', 'varchar(255) NOT NULL')],
            s3_url="s3://edx-test/test/",
            overwrite=False
        )


def test_load_s3_data_to_mysql_overwrite_without_record_filter(mock_mysql_connection):
    mock_cursor = mock_mysql_connection.cursor()
    mock_fetchone = mock.Mock()
    mock_cursor.fetchone = mock_fetchone

    with Flow("test") as f:
        utils_mysql.load_s3_data_to_mysql(
            aurora_credentials={},
            database="test_database",
            table="test_table",
            table_columns=[('id', 'int'), ('course_id', 'varchar(255) NOT NULL')],
            s3_url="s3://edx-test/test/",
            overwrite=True
        )

    state = f.run()
    assert state.is_successful()
    mock_cursor.execute.assert_has_calls(
        [
            mock.call("SELECT 1 FROM test_table  LIMIT 1"),
            mock.call("DELETE FROM test_table ")
        ]
    )


def test_load_s3_data_to_mysql_overwrite_with_record_filter(mock_mysql_connection):
    mock_cursor = mock_mysql_connection.cursor()
    mock_fetchone = mock.Mock()
    mock_cursor.fetchone = mock_fetchone

    with Flow("test") as f:
        utils_mysql.load_s3_data_to_mysql(
            aurora_credentials={},
            database="test_database",
            table="test_table",
            table_columns=[('id', 'int'), ('course_id', 'varchar(255) NOT NULL')],
            s3_url="s3://edx-test/test/",
            record_filter="where course_id='edX/Open_DemoX/edx_demo_course'",
            overwrite=True
        )

    state = f.run()
    assert state.is_successful()
    mock_cursor.execute.assert_has_calls(
        [
            mock.call("SELECT 1 FROM test_table where course_id='edX/Open_DemoX/edx_demo_course' LIMIT 1"),
            mock.call("DELETE FROM test_table where course_id='edX/Open_DemoX/edx_demo_course'")
        ]
    )


def test_load_s3_data_to_mysql(mock_mysql_connection):
    mock_cursor = mock_mysql_connection.cursor()
    mock_fetchone = mock.Mock()
    mock_cursor.fetchone = mock_fetchone

    with Flow("test") as f:
        utils_mysql.load_s3_data_to_mysql(
            aurora_credentials={},
            database="test_database",
            table="test_table",
            table_columns=[('id', 'int'), ('course_id', 'varchar(255) NOT NULL')],
            s3_url="s3://edx-test/test/",
            record_filter="where course_id='edX/Open_DemoX/edx_demo_course'",
            ignore_num_lines=2,
            overwrite=True
        )

    state = f.run()
    assert state.is_successful()
    mock_cursor.execute.assert_has_calls(
        [
            mock.call("\n        CREATE TABLE IF NOT EXISTS test_table (id int,course_id varchar(255) NOT NULL)\n    "), # noqa
            mock.call("SELECT 1 FROM test_table where course_id='edX/Open_DemoX/edx_demo_course' LIMIT 1"), # noqa
            mock.call("DELETE FROM test_table where course_id='edX/Open_DemoX/edx_demo_course'"), # noqa
            mock.call("\n            LOAD DATA FROM S3 PREFIX 's3://edx-test/test/'\n            INTO TABLE test_table\n            FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY ''\n            ESCAPED BY '\\\\'\n            IGNORE 2 LINES\n        "), # noqa
        ]
    )


def test_load_s3_data_to_mysql_overwrite_with_temp_table(mock_mysql_connection):
    mock_cursor = mock_mysql_connection.cursor()

    with Flow("test") as f:
        utils_mysql.load_s3_data_to_mysql(
            aurora_credentials={},
            database="test_database",
            table="test_table",
            table_columns=[('id', 'int'), ('course_id', 'varchar(255) NOT NULL')],
            s3_url="s3://edx-test/test/",
            overwrite=True,
            overwrite_with_temp_table=True,
        )

    state = f.run()
    assert state.is_successful()
    mock_cursor.execute.assert_has_calls(
        [
            mock.call("\n        CREATE TABLE IF NOT EXISTS test_table (id int,course_id varchar(255) NOT NULL)\n    "),
            mock.call("SELECT 1 FROM test_table  LIMIT 1"),
            mock.call("DROP TABLE IF EXISTS test_table_old"),
            mock.call("DROP TABLE IF EXISTS test_table_temp"),
            mock.call("CREATE TABLE test_table_temp (id int,course_id varchar(255) NOT NULL)"),
            mock.call("\n            LOAD DATA FROM S3 PREFIX 's3://edx-test/test/'\n            INTO TABLE test_table_temp\n            FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY ''\n            ESCAPED BY '\\\\'\n            IGNORE 0 LINES\n        "), # noqa
            mock.call("RENAME TABLE test_table to test_table_old, test_table_temp to test_table"),
            mock.call("DROP TABLE IF EXISTS test_table_old"),
            mock.call("DROP TABLE IF EXISTS test_table_temp"),
        ]
    )


def test_table_creation_with_indexes(mock_mysql_connection):
    mock_cursor = mock_mysql_connection.cursor()
    with Flow("test") as f:
        utils_mysql.load_s3_data_to_mysql(
            aurora_credentials={},
            database="test_database",
            table="test_table",
            table_columns=[('user_id', 'int'), ('course_id', 'varchar(255) NOT NULL')],
            table_indexes=[('user_id', ), ('course_id', ), ('user_id', 'course_id')],
            s3_url="s3://edx-test/test/",
            overwrite=True,
            overwrite_with_temp_table=True,
        )

    state = f.run()
    assert state.is_successful()
    mock_cursor.execute.assert_has_calls(
        [
            mock.call("\n        CREATE TABLE IF NOT EXISTS test_table (user_id int,course_id varchar(255) NOT NULL,INDEX (user_id),INDEX (course_id),INDEX (user_id,course_id))\n    "), # noqa
        ]
    )
