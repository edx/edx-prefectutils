
import backoff
import requests
from sailthru.sailthru_client import SailthruClient
from prefect import task


from . import snowflake


@task
@backoff.on_exception(backoff.expo,
                      snowflake.connector.ProgrammingError,
                      max_tries=3)
def sync_sailthru_to_braze(
    sf_credentials: dict,
    sf_role: str,
    sf_database: str,
    sf_schema: str,
    sf_table: str,
    braze_api_key: str,
    braze_api_server: str,
):
    sf_connection = snowflake.create_snowflake_connection(
        sf_credentials,
        sf_role,
    )
    query = """
    SELECT email FROM {table}
    """.format(
        table=snowflake.qualified_table_name(sf_database, sf_schema, sf_table),
    )
    cursor = sf_connection.cursor()
    cursor.execute(query)
    # 50 email batches because that's how many braze accepts at a time
    for rows in cursor.fetchmany(50):
        requests.post(
            url=f"{braze_api_server}/email/status",
            json={
                'email': [row['email'] for row in rows],
                'subscription_state': 'unsubscribed',
            },
            headers={'Authorization': f'BEARER {braze_api_key}'}
        )


@task
@backoff.on_exception(backoff.expo,
                      snowflake.connector.ProgrammingError,
                      max_tries=3)
def sync_braze_to_sailthru(
    sf_credentials: dict,
    sf_role: str,
    sf_database: str,
    sf_schema: str,
    sf_table: str,
    sailthru_api_key: str,
    sailthru_api_secret: str,
):
    sailthru_client = SailthruClient(sailthru_api_key, sailthru_api_secret)
    sf_connection = snowflake.create_snowflake_connection(
        sf_credentials,
        sf_role,
    )
    query = """
    SELECT email FROM {table}
    """.format(
        table=snowflake.qualified_table_name(sf_database, sf_schema, sf_table),
    )
    cursor = sf_connection.cursor()
    cursor.execute(query)
    # 50 email batches because that's how many braze accepts at a time
    for rows in cursor.fetchmany():
        for row in rows:
            sailthru_client.save_user(row['email'], {'optout_email': 'all'})
