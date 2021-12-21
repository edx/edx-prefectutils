"""
Utility methods and tasks for working with Snowflake from a Prefect flow.
"""
import json
import os
from collections import namedtuple
from typing import List, TypedDict
from urllib.parse import urlparse

import backoff
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from prefect import task
from prefect.engine import signals
from prefect.tasks.aws import s3
from prefect.utilities.logging import get_logger

from edx_prefectutils import s3 as s3_utils

MANIFEST_FILE_NAME = 'manifest.json'


class SFCredentials(TypedDict, total=False):
    private_key: str
    private_key_passphrase: str
    user: str
    account: str
    password: str


def create_snowflake_connection(
    credentials: SFCredentials,
    role: str,
    autocommit: bool = False,
    warehouse: str = None
) -> snowflake.connector.SnowflakeConnection:
    """
    Connects to the Snowflake database.

      credentials (SFCredentials):
        Snowflake credentials including key & passphrase, along with user and account.
      role (str): Name of the role to use for the connection.
      autocommit (bool): True to enable autocommit for the connection, False if not.
      warehouse (str): The Snowflake warehouse to use for this connection. Defaults to the user's default warehouse.
    """
    private_key = credentials.get("private_key")

    private_key_passphrase = credentials.get("private_key_passphrase")
    user = credentials.get("user")
    account = credentials.get("account")
    password = credentials.get("password")

    p_key = None
    pkb = None
    if private_key and private_key_passphrase:
        p_key = serialization.load_pem_private_key(
            private_key.encode(),
            password=private_key_passphrase.encode(),
            backend=default_backend(),
        )

        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    connection = snowflake.connector.connect(
        user=user, account=account, private_key=pkb, password=password, autocommit=autocommit, warehouse=warehouse
    )

    # Switch to specified role.
    connection.cursor().execute("USE ROLE {}".format(role))
    # Set timezone to UTC
    connection.cursor().execute("ALTER SESSION SET TIMEZONE = 'UTC'")

    return connection


def qualified_table_name(database, schema, table) -> str:
    """
    Fully qualified Snowflake table name.
    """
    return "{database}.{schema}.{table}".format(
        database=database, schema=schema, table=table
    )


def qualified_stage_name(database, schema, table) -> str:
    """
    Fully qualified Snowflake stage name.
    """
    return "{database}.{schema}.{table}_stage".format(
        database=database, schema=schema, table=table,
    )


@task
@backoff.on_exception(backoff.expo,
                      snowflake.connector.ProgrammingError,
                      max_tries=3)
def load_ga_data_to_snowflake(
    sf_credentials: SFCredentials,
    sf_database: str,
    sf_schema: str,
    sf_table: str,
    sf_role: str,
    sf_warehouse: str,
    sf_storage_integration: str,
    bq_dataset: str,
    gcs_url: str,
    date: str,
    pattern: str = ".*",
    overwrite: bool = False,
):
    """
    Loads JSON objects from GCS to Snowflake.
    Args:
      sf_credentials (SFCredentials):
        Snowflake public key credentials in the format required by create_snowflake_connection.
      sf_database (str): Name of the destination database.
      sf_schema (str): Name of the destination schema.
      sf_table (str): Name of the destination table.
      sf_role (str): Name of the snowflake role to assume.
      sf_warehouse (str): Name of the Snowflake warehouse to be used for loading.
      sf_storage_integration (str):
        The name of the pre-configured storage integration created for this flow.
      bq_dataset (str): BQ Dataset to which this load belongs to. This gets set as `ga_view_id' in the dest. table.
      gcs_url (str): Full URL to the GCS path containing the files to load.
      pattern (str, optional): Path pattern/regex to match GCS object to copy.
      date (str): Date of `ga_sessions` being loaded.
      overwrite (bool, optional): Whether to overwrite existing data for the given date. Defaults to `False`.
    """
    sf_connection = create_snowflake_connection(sf_credentials, sf_role)
    # Snowflake expects GCS locations to start with `gcs` instead of `gs`.
    gcs_url = gcs_url.replace("gs://", "gcs://")

    # Check for data existence for this date
    try:
        query = """
        SELECT 1 FROM {table}
        WHERE session:date='{date}'
            AND ga_view_id='{ga_view_id}'
        """.format(
            table=qualified_table_name(sf_database, sf_schema, sf_table),
            date=date,
            ga_view_id=bq_dataset,
        )
        cursor = sf_connection.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
    except snowflake.connector.ProgrammingError as e:
        if "does not exist" in e.msg:
            # If so then the query failed because the table doesn't exist.
            row = None
        else:
            raise

    if row and not overwrite:
        return

    try:
        query = """
        CREATE TABLE IF NOT EXISTS {table} (
            id number autoincrement start 1 increment 1,
            load_time timestamp_ltz default current_timestamp(),
            ga_view_id string,
            session VARIANT
        );
        """.format(
            table=qualified_table_name(sf_database, sf_schema, sf_table)
        )
        sf_connection.cursor().execute(query)

        if overwrite:
            query = """
            DELETE FROM {table}
            WHERE session:date='{date}'
                AND ga_view_id='{ga_view_id}'
            """.format(
                table=qualified_table_name(sf_database, sf_schema, sf_table),
                date=date,
                ga_view_id=bq_dataset,
            )
            sf_connection.cursor().execute(query)

        query = """
        CREATE OR REPLACE STAGE {stage_name}
            URL = '{stage_url}'
            STORAGE_INTEGRATION = {storage_integration}
            FILE_FORMAT = (TYPE = JSON);
        """.format(
            stage_name=qualified_stage_name(sf_database, sf_schema, sf_table),
            stage_url=gcs_url,
            storage_integration=sf_storage_integration,
        )
        sf_connection.cursor().execute(query)

        query = """
        COPY INTO {table} (ga_view_id, session)
            FROM (
                SELECT
                    '{ga_view_id}',
                    t.$1
                FROM @{stage_name} t
            )
        PATTERN='{pattern}'
        FORCE={force}
        """.format(
            table=qualified_table_name(sf_database, sf_schema, sf_table),
            ga_view_id=bq_dataset,
            stage_name=qualified_stage_name(sf_database, sf_schema, sf_table),
            pattern=pattern,
            force=str(overwrite),
        )
        sf_connection.cursor().execute(query)
        sf_connection.commit()
    except Exception:
        sf_connection.rollback()
        raise
    finally:
        sf_connection.close()


@task
def load_s3_data_to_snowflake(
    date: str,
    date_property: str,
    sf_credentials: SFCredentials,
    sf_database: str,
    sf_schema: str,
    sf_table: str,
    sf_role: str,
    sf_warehouse: str,
    sf_storage_integration_name: str,
    s3_url: str,
    sf_file_format: str = "TYPE='JSON', STRIP_OUTER_ARRAY=TRUE",
    file: str = None,
    pattern: str = None,
    overwrite: bool = False,
    truncate: bool = False,
    disable_existence_check: bool = False,
):
    """
    Loads objects in S3 to a generic table in Snowflake, the data is stored in a variant column named
    `PROPERTIES`. Note that either `file` or `pattern` parameter must be specified.
    Notes:
      To load a single file, use the `file` parameter.
      You must explicitly include a separator (/) either at the end of the `s3_url` or at the beginning
      of file path specified in the `file` parameter. Typically you would include a trailing (/)
      in `s3_url`.
      To load multiple files, use `pattern`.
    Examples:
      To load a single file `s3://bucket/path/to/filename/filename.ext`:
      load_s3_data_to_snowflake(s3_url='s3://bucket/path/to/filename/, file='filename.ext')
      or
      load_s3_data_to_snowflake(s3_url='s3://bucket/path/, file='to/filename/filename.ext')

      Load all files under `s3://bucket/path/` with a certain name:
      load_s3_data_to_snowflake(s3_url='s3://bucket/path/, pattern='.*filename.ext')

    Args:
      date (str): Date of the data being loaded.
      date_property (str): Date type property name in the variant `PROPERTIES` column.
      sf_credentials (SFCredentials): Snowflake public key credentials in the
              format required by create_snowflake_connection.
      sf_database (str): Name of the destination database.
      sf_schema (str): Name of the destination schema.
      sf_table (str): Name of the destination table.
      sf_role (str): Name of the snowflake role to assume.
      sf_warehouse (str): Name of the Snowflake warehouse to be used for loading.
      sf_storage_integration_name (str): Name of the Snowflake storage integration to use. These are
              configured in Terraform.
      s3_url (str): Full URL to the S3 path containing the files to load.
      sf_file_format (str, optional): Snowflake file format for the Stage. Defaults to 'JSON'.
      file (str, optional): File path relative to `s3_url`.
      pattern (str, optional): Path pattern/regex to match S3 objects to copy. Defaults to `None`.
      overwrite (bool, optional): Whether to overwrite existing data for the given date. Defaults to `False`.
      truncate (bool, optional): Whether to truncate the table. Defaults to `False`.
      disable_existence_check (bool, optional): Whether to disable check for existing data, useful when
              always appending to the table regardless of any existing data for that provided `date`
    """
    logger = get_logger()
    if not file and not pattern:
        raise signals.FAIL('Either `file` or `pattern` must be specified to run this task.')

    sf_connection = create_snowflake_connection(sf_credentials, sf_role, warehouse=sf_warehouse)

    if truncate:
        query = "TRUNCATE IF EXISTS {}".format(qualified_table_name(sf_database, sf_schema, sf_table))
        logger.info("Truncating table: {}".format(sf_table))
        cursor = sf_connection.cursor()
        cursor.execute(query)

    # Check for data existence for this date
    row = None
    if not disable_existence_check:
        try:
            query = """
            SELECT 1 FROM {table}
            WHERE date(PROPERTIES:{date_property})=date('{date}')
            """.format(
                table=qualified_table_name(sf_database, sf_schema, sf_table),
                date=date,
                date_property=date_property,
            )

            logger.info("Checking existence of data for {}".format(date))

            cursor = sf_connection.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
        except snowflake.connector.ProgrammingError as e:
            if "does not exist" in e.msg:
                # If so then the query failed because the table doesn't exist.
                row = None
            else:
                raise

    if row and not overwrite:
        raise signals.SKIP('Skipping task as data for the date exists and no overwrite was provided.')
    else:
        logger.info("Continuing with S3 load for {}".format(date))

    try:
        # Create the generic loading table
        query = """
        CREATE TABLE IF NOT EXISTS {table} (
            ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
            LOAD_TIME TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
            ORIGIN_FILE_NAME VARCHAR(16777216),
            ORIGIN_FILE_LINE NUMBER(38,0),
            ORIGIN_STR VARCHAR(16777216),
            PROPERTIES VARIANT
        );
        """.format(
            table=qualified_table_name(sf_database, sf_schema, sf_table)
        )

        sf_connection.cursor().execute(query)

        # Delete existing data in case of overwrite.
        if overwrite and row:
            logger.info("Deleting data for overwrite for {}".format(date))

            query = """
            DELETE FROM {table}
            WHERE date(PROPERTIES:{date_property})=date('{date}')
            """.format(
                table=qualified_table_name(sf_database, sf_schema, sf_table),
                date=date,
                date_property=date_property,
            )
            sf_connection.cursor().execute(query)

        # Create stage
        query = """
        CREATE STAGE IF NOT EXISTS {stage_name}
            URL = '{stage_url}'
            STORAGE_INTEGRATION = {storage_integration}
            FILE_FORMAT = ({file_format});
        """.format(
            stage_name=qualified_stage_name(sf_database, sf_schema, sf_table),
            stage_url=s3_url,
            storage_integration=sf_storage_integration_name,
            file_format=sf_file_format,
        )
        sf_connection.cursor().execute(query)

        files_paramater = ""
        pattern_parameter = ""

        if file:
            logger.info("Loading file {}".format(file))
            files_paramater = "FILES = ( '{}' )".format(file)

        if pattern:
            logger.info("Loading pattern {}".format(pattern))
            pattern_parameter = "PATTERN = '{}'".format(pattern)

        query = """
        COPY INTO {table} (origin_file_name, origin_file_line, origin_str, properties)
            FROM (
                SELECT
                    metadata$filename,
                    metadata$file_row_number,
                    t.$1,
                    CASE
                        WHEN CHECK_JSON(t.$1) IS NULL THEN t.$1
                        ELSE NULL
                    END
                FROM @{stage_name} t
            )
        {files_parameter}
        {pattern_parameter}
        FORCE={force}
        """.format(
            table=qualified_table_name(sf_database, sf_schema, sf_table),
            stage_name=qualified_stage_name(sf_database, sf_schema, sf_table),
            files_parameter=files_paramater,
            pattern_parameter=pattern_parameter,
            force=str(overwrite),
        )

        logger.info("Copying data into Snowflake as: \n{}".format(query))

        sf_connection.cursor().execute(query)
        sf_connection.commit()
    except Exception:
        sf_connection.rollback()
        raise
    finally:
        sf_connection.close()


@task
def export_snowflake_table_to_s3(
    sf_credentials: SFCredentials,
    sf_database: str,
    sf_schema: str,
    sf_table: str,
    sf_role: str,
    sf_warehouse: str,
    sf_storage_integration: str,
    s3_path: str,
    field_delimiter: str = ',',
    enclosed_by: str = 'NONE',
    escape_unenclosed_field: str = '\\\\',
    null_marker: str = 'NULL',
    overwrite: bool = True,
    single: bool = False,
    generate_manifest: bool = False,
):

    """
    Exports a snowflake table to S3.
    The output path is generated by appending fully qualified table name to `s3_path` parameter.

    Args:
      sf_credentials (SFCredentials): Snowflake public key credentials in the format
              required by create_snowflake_connection.
      sf_database (str): Name of the source database.
      sf_schema (str): Name of the source schema.
      sf_table (str): Name of the source table.
      sf_role (str): Name of the snowflake role to assume.
      sf_warehouse (str): Name of the Snowflake warehouse to be used for loading.
      sf_storage_integration_name (str): Name of the Snowflake storage integration to use. These are
              configured in Terraform.
      s3_path (str): S3 base path used to unload the table, this will be appended with the qualified table name.
      field_delimiter (str, optional): The character to use for separating fields in the output file. Defaults to `,`.
      enclosed_by (str, optional): Character used to enclose strings. Defaults to `NONE`
      escape_unenclosed_field (str, optional): Single character string used as the escape character for unenclosed
              field values only. Defaults to snowflake default `\\`.
      null_marker (str, optional): String used to convert SQL NULL. Defaults to `NULL`.
      overwrite (bool, optional): Whether to overwrite existing data in S3. Defaults to `TRUE`.
      single (bool, optional): Whether to generate a single file in S3. Defaults to `FALSE`. The maximum file size
              for a single file defaults to 16MB, although that default can be updated by adding a MAX_FILE_SIZE
              copy option.
      generate_manifest (bool, optional): Whether to generate a manifest file in S3. Defaults to `FALSE`.
    """
    logger = get_logger()

    sf_connection = create_snowflake_connection(
        credentials=sf_credentials,
        role=sf_role,
        warehouse=sf_warehouse,
    )

    table_name = qualified_table_name(sf_database, sf_schema, sf_table)
    export_path = os.path.join(s3_path, table_name.replace('.', '-').lower()) + '/'

    parsed_path = urlparse(export_path)
    export_bucket = parsed_path.netloc
    export_prefix = parsed_path.path.lstrip('/')

    # Snowflake's COPY INTO command's OVERWRITE option is not reliable, so we need to delete the data manually
    if overwrite:
        logger.info("Deleting existing data in S3 bucket: {bucket} with prefix: {prefix}".format(
            bucket=export_bucket, prefix=export_prefix))
        s3_utils.delete_s3_directory.run(export_bucket, export_prefix)

    query = """
        COPY INTO '{export_path}'
            FROM {table}
            STORAGE_INTEGRATION = {storage_integration}
            FILE_FORMAT = ( TYPE = CSV EMPTY_FIELD_AS_NULL = FALSE
            FIELD_DELIMITER = '{field_delimiter}' FIELD_OPTIONALLY_ENCLOSED_BY = '{enclosed_by}'
            ESCAPE_UNENCLOSED_FIELD = '{escape_unenclosed_field}'
            NULL_IF = ( '{null_marker}' )
            COMPRESSION = NONE
            )
            OVERWRITE={overwrite}
            SINGLE={single}
            DETAILED_OUTPUT = TRUE
    """.format(
        export_path=export_path,
        table=table_name,
        storage_integration=sf_storage_integration,
        field_delimiter=field_delimiter,
        enclosed_by=enclosed_by,
        escape_unenclosed_field=escape_unenclosed_field,
        null_marker=null_marker,
        overwrite=overwrite,
        single=single
    )
    logger.info(query)
    cursor = sf_connection.cursor()

    try:
        cursor.execute(query)
        if generate_manifest:
            # With DETAILED_OUTPUT=TRUE, the COPY command returns rows for each file that gets created.
            # The first column is the FILE_NAME
            s3_file_names = [row[0] for row in cursor.fetchall()]
            # Generate a manifest file in S3 which can be later used by LOAD DATA FROM S3 MANIFEST
            # command to load the data into MySQL.
            s3_file_paths = [os.path.join(export_path, s3_file_name) for s3_file_name in s3_file_names]
            s3_manifest_file_prefix = os.path.join(export_prefix, MANIFEST_FILE_NAME)
            manifest_file_content = {
                "entries": [
                    {"url": s3_file_path, "mandatory": True} for s3_file_path in s3_file_paths
                ]
            }

            s3.S3Upload(bucket=export_bucket).run(
                json.dumps(manifest_file_content),
                key=s3_manifest_file_prefix
            )
    except snowflake.connector.ProgrammingError as e:
        if 'Files already existing at the unload destination' in e.msg:
            logger.error("Files already exist at {destination}".format(destination=export_path))
            raise signals.FAIL('Files already exist. Use overwrite option to force unloading.')
        else:
            raise
    finally:
        sf_connection.close()

    return export_path


def get_batched_rows_from_snowflake(
    sf_credentials: SFCredentials,
    sf_database: str,
    sf_schema: str,
    sf_table: str,
    sf_role: str,
    columns: List[str],
    batch_size: int,
    where: str = None,
):
    """
    Query batches of rows from snowflake as a generator.

    Args:
      sf_credentials (SFCredentials): Snowflake public key credentials in the format
              required by create_snowflake_connection.
      sf_database (str): Name of the source database.
      sf_schema (str): Name of the source schema.
      sf_table (str): Name of the source table.
      sf_role (str): Name of the snowflake role to assume.
      columns (list[str]): The column field names you want to select.
      batch_size (int): How many at most to return at once.
      where (str, optional): A WHERE query string for the snowflake SELECT call. None will return all rows.

    Returns:
      A generator that yields lists of row objects. Each list will be at most `batch_size` long.
      Each row object will be a named tuple with the field names of the provided `columns`.

    Examples:
      for rows in get_batched_rows_from_snowflake(..., ['foo', 'bar'], 75):
        for row in rows:
          print('Got foo %s and bar %s', row.foo, row.bar)

      for rows in get_batched_rows_from_snowflake(..., ['foo'], 50, where='bar is null'):
        for row in rows:
          print('Got foo %s', row.foo)
    """
    sf_connection = create_snowflake_connection(
        sf_credentials,
        sf_role,
    )

    query = 'SELECT {columns} FROM {table}'.format(
        columns=', '.join(columns),
        table=qualified_table_name(sf_database, sf_schema, sf_table),
    )
    if where:
        query += ' WHERE ' + where

    cursor = sf_connection.cursor()
    cursor.execute(query)

    SnowFlakeRow = namedtuple('SnowFlakeRow', columns)
    batch = cursor.fetchmany(batch_size)
    while len(batch) > 0:
        yield [SnowFlakeRow(*row) for row in batch]
        batch = cursor.fetchmany(batch_size)
