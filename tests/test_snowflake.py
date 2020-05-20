#!/usr/bin/env python

"""
Tests for Snowflake utils in the `prefect_utils` package.
"""

import mock
from pytest_mock import mocker  # noqa: F401
from prefect_utils import snowflake


def test_qualified_table_name():
    assert 'test_db.test_schema.test_table' == snowflake.qualified_table_name(
        'test_db', 'test_schema', 'test_table'
    )


def test_qualified_stage_name():
    assert 'test_db.test_schema.test_table_stage' == snowflake.qualified_stage_name(
        'test_db', 'test_schema', 'test_table'
    )


def test_create_snowflake_connection(mocker):  # noqa: F811
    # Mock the Snowflake connection and cursor.
    mocker.patch.object(snowflake.snowflake.connector, 'connect')
    mock_cursor = mocker.Mock()
    mock_connection = mocker.Mock()
    mock_connection.cursor.return_value = mock_cursor
    snowflake.snowflake.connector.connect.return_value = mock_connection
    # Mock the key decryption.
    mocker.patch.object(snowflake.serialization, 'load_pem_private_key')
    mock_key = mocker.Mock()
    mock_key.private_bytes.return_value = 1234
    snowflake.serialization.load_pem_private_key.return_value = mock_key
    # Call the connection method.
    snowflake.create_snowflake_connection(
        credentials={
            "private_key": "this_is_an_encrypted_private_key",
            "private_key_passphrase": "passphrase_for_the_private_key",
            "user": "test_user",
            "account": "company-cloud-region"
        },
        role="test_role"
    )
    snowflake.snowflake.connector.connect.assert_called_with(
        account='company-cloud-region',
        autocommit=False,
        private_key=1234,
        user='test_user'
    )
    mock_cursor.execute.assert_has_calls(
        (
            mock.call("USE ROLE test_role"),
            mock.call("ALTER SESSION SET TIMEZONE = 'UTC'"),
        )
    )


def test_load_json_objects_to_snowflake(mocker):  # noqa: F811
    pass
