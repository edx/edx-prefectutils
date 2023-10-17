"""
Tasks for interacting with Aurora MySQL.
"""
import os

import mysql.connector
from prefect import task
from prefect.engine import signals
from prefect.utilities.logging import get_logger

from edx_prefectutils.s3 import get_s3_csv_column_names
from edx_prefectutils.snowflake import MANIFEST_FILE_NAME


def create_mysql_connection(credentials: dict, database: str, autocommit: bool = False):

    user = credentials['user']
    password = credentials['password']
    host = credentials['host']

    try:
        connection = mysql.connector.connect(
            user=user,
            password=password,
            host=host,
            database=database,
            autocommit=autocommit,
        )
    except mysql.connector.errors.ProgrammingError as err:
        if 'Unknown database' in err.msg:
            # Create the database if it doesn't exist.
            connection = mysql.connector.connect(
                user=user,
                password=password,
                host=host,
                autocommit=autocommit,
            )
            cursor = connection.cursor()
            cursor.execute(f'CREATE DATABASE IF NOT EXISTS {database}')
            cursor.execute(f'USE {database}')
            cursor.close()
        else:
            raise err

    return connection


def get_columns_load_order(s3_url: str, table_name: str, table_column_names: list):
    """
    Return list of column names to tell `LOAD DATA` command the order in which to load data from csv.

    NOTE: This logic is based on `col_name_or_user_var` option provide by `LOAD DATA` command.
    Please see https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraMySQL.Integrating.LoadFromS3.html
    """
    csv_column_names = get_s3_csv_column_names(s3_url)

    # We can load csv data into mysql table only if
    # 1. csv_column_names == table_column_names
    # 2. csv_column_names and table_column_names have same column names but order does not match
    # 3. csv_column_names list have more columns and extra columns are at the end of csv_column_names

    # case 1 and 2
    if csv_column_names == table_column_names or sorted(csv_column_names) == sorted(table_column_names):
        return csv_column_names

    # case 3
    # find extra columns in csv_column_names
    extra_columns = [column for column in csv_column_names if column not in table_column_names]
    # remove extra columns from csv_column_names
    remaining_column_names = csv_column_names[0:-len(extra_columns)]
    # remaining_columns_names must be equal to table_column_names if extra_columns are present at the end
    if sorted(remaining_column_names) == sorted(table_column_names):
        return remaining_column_names

    raise ValueError(
        'Can not load [{}] to [{}]. Fields mismatch. CSVFields: [{}], TableFields: [{}]'.format(
            s3_url,
            table_name,
            csv_column_names,
            table_column_names
        )
    )


@task
def load_s3_data_to_mysql(
    aurora_credentials: dict,
    database: str,
    s3_url: str,
    table: str,
    table_columns: list,
    table_indexes: list = [],
    field_delimiter: str = ',',
    enclosed_by: str = '',
    escaped_by: str = '\\\\',
    record_filter: str = '',
    ignore_num_lines: int = 0,
    overwrite: bool = False,
    overwrite_with_temp_table: bool = False,
    use_manifest: bool = False,
    load_in_order: bool = False,
):

    """
    Loads an Aurora MySQL database table from text files in S3.

    Args:
      aurora_credentials (dict): Aurora credentials dict containing `user`, `password` and `host`.
      database (str): Name of the destination database.
      s3_url (str): Full S3 URL containing the files to load into the destination table .
      table (str): Name of the destination table.
      table_columns (list): List of tuples specifying table schema.
              Example: `[('id', 'int'), ('course_id', 'varchar(255) NOT NULL')]`
      table_indexes (list): List of tuples specifying table indexes to add. Defaults to `[]`.
              Example: `[('user_id',), ('course_id',), ('user_id', 'course_id')]`
      field_delimiter (str, optional): The character used to indicate how the fields in input files are delimited.
              Defaults to `,`.
      enclosed_by (str, optional): Single character string that specifies the fields enclosing character.
              Defaults to empty string(no enclosing character).
      escaped_by (str, optional): Single character string which indicates the escaping of delimiters and
              other escape sequences. Defaults to backslash(`\\`).
      record_filter (str, optional): Entire `WHERE` clause which specifies the data to overwrite. An empty value
              with overwrite=True will delete all the rows from the table.
      ignore_num_lines (int, optional): Specifies to ignore a certain number of lines at the start of the input file.
              Defaults to 0.
      overwrite (bool, optional): Whether to overwrite existing data in the destination Table. Defaults to `False`.
      overwrite_with_temp_table (bool, optional): Whether to use a temporary table to overwrite data instead of
              `DELETE`. The data would first be loaded into a new table followed by an atomic rename. Use this option
              if there are any schema changes or for expensive `DELETE` operations.
              IMPORTANT: Do not use this option for incrementally updated tables as any historical data would be lost.
                Defaults to `False`.
      use_manifest (bool, optional): Whether to use a manifest file to load data. Defaults to `False`.
      load_in_order (bool, optional): Load data into mysql table according to the column ordering in csv file.
    """

    def _drop_temp_tables(table, connection):
        for table in [table + '_old', table + '_temp']:
            query = "DROP TABLE IF EXISTS {table}".format(table=table)
            connection.cursor().execute(query)

    logger = get_logger()

    connection = create_mysql_connection(aurora_credentials, database)

    table_schema = []

    table_schema.extend(table_columns)

    for indexed_cols in table_indexes:
        table_schema.append(("INDEX", "({cols})".format(cols=','.join(indexed_cols))))

    table_schema = ','.join(
        '{name} {definition}'.format(name=name, definition=definition) for name, definition in table_schema
    )

    # Create the table if it does not exist
    query = """
        CREATE TABLE IF NOT EXISTS {table} ({table_schema})
    """.format(
        table=table,
        table_schema=table_schema
    )
    logger.debug(query)
    connection.cursor().execute(query)

    # Check for existing data
    query = "SELECT 1 FROM {table} {record_filter} LIMIT 1".format(table=table, record_filter=record_filter)
    cursor = connection.cursor()
    cursor.execute(query)
    row = cursor.fetchone()

    if row and not overwrite:
        raise signals.SKIP('Skipping task as data already exists in the dest. table and no overwrite was provided.')

    # Create a temp table for loading data
    if overwrite and overwrite_with_temp_table:
        _drop_temp_tables(table, connection)
        query = "CREATE TABLE {table} ({table_schema})".format(table=table + '_temp', table_schema=table_schema)
        connection.cursor().execute(query)

    columns_load_order = ''
    if load_in_order:
        table_column_names = [name for name, __ in table_columns]
        columns_to_load = get_columns_load_order(s3_url, table, table_column_names)
        columns_load_order = '( {} )'.format(', '.join(columns_to_load))
        logger.info('MySQL column load order: {}'.format(columns_load_order))

    try:
        if row and overwrite and not overwrite_with_temp_table:
            query = "DELETE FROM {table} {record_filter}".format(table=table, record_filter=record_filter)
            logger.debug("Deleting existing data for {table}".format(table=table))
            connection.cursor().execute(query)

        if use_manifest:
            s3_url = os.path.join(s3_url, MANIFEST_FILE_NAME)
            prefix_or_manifest = "MANIFEST"
        else:
            prefix_or_manifest = "PREFIX"

        query = """
            LOAD DATA FROM S3 {prefix_or_manifest} '{s3_url}'
            INTO TABLE {table}
            FIELDS TERMINATED BY '{delimiter}' OPTIONALLY ENCLOSED BY '{enclosed_by}'
            ESCAPED BY '{escaped_by}'
            IGNORE {ignore_lines} LINES
        """.format(
            prefix_or_manifest=prefix_or_manifest,
            s3_url=s3_url,
            table=table if not overwrite_with_temp_table else table + '_temp',
            delimiter=field_delimiter,
            enclosed_by=enclosed_by,
            escaped_by=escaped_by,
            ignore_lines=ignore_num_lines,
        )
        connection.cursor().execute(query)

        if overwrite and overwrite_with_temp_table:
            # Note that this would cause an implicit commit.
            query = "RENAME TABLE {table} to {table_old}, {table_temp} to {table}".format(
                table=table, table_old=table + '_old', table_temp=table + '_temp'
            )
            connection.cursor().execute(query)
        else:
            # Commit if we're not getting an implicit commit from RENAME.
            connection.commit()
    except Exception as e:
        logger.error(str(e))
        connection.rollback()
        raise
    finally:
        _drop_temp_tables(table, connection)
        connection.close()
