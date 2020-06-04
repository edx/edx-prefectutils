"""
Utility methods and tasks for working with Snowflake from a Prefect flow.
"""
import backoff
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from prefect import task


def create_snowflake_connection(
    credentials: dict,
    role: str,
    autocommit=False
) -> snowflake.connector.SnowflakeConnection:
    """
    Connects to the Snowflake database.

      credentials (dict):
        Snowflake credentials including key & passphrase, along with user and account.
      role (str): Name of the role to use for the connection.
      autocommit (bool): True to enable autocommit for the connection, False if not.
    """
    private_key = credentials.get("private_key")

    private_key_passphrase = credentials.get("private_key_passphrase")
    user = credentials.get("user")
    account = credentials.get("account")

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
        user=user, account=account, private_key=pkb, autocommit=autocommit
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
    sf_credentials: dict,
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
      sf_credentials (dict):
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
