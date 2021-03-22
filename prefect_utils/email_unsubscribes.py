
import backoff
import prefect
import requests
from prefect import task
from sailthru.sailthru_client import SailthruClient
from sailthru.sailthru_error import SailthruClientError
from snowflake.connector import ProgrammingError

from . import snowflake


@task
@backoff.on_exception(backoff.expo,
                      ProgrammingError,
                      max_tries=3)
def sync_sailthru_to_braze(
    sf_credentials: snowflake.SFCredentials,
    sf_role: str,
    sf_database: str,
    sf_schema: str,
    braze_api_key: str,
    braze_api_server: str,
):
    logger = prefect.context.get("logger")

    sf_connection = snowflake.create_snowflake_connection(
        sf_credentials,
        sf_role,
    )
    sf_table = 'email_unsubscribes_sailthru_to_braze'
    query = """
    SELECT email FROM {table}
    """.format(
        table=snowflake.qualified_table_name(sf_database, sf_schema, sf_table),
    )
    cursor = sf_connection.cursor()
    cursor.execute(query)
    count = 0
    # 50 email batches because that's how many braze accepts at a time
    batch = cursor.fetchmany(50)
    while len(batch) > 0:
        unsubscribe_emails_braze(
            braze_api_server,
            braze_api_key,
            [row[0] for row in batch]
        )
        count += len(batch)
        if count % 500 == 0:
            logger.info("Unsubscribed %d emails from braze", count)
        batch = cursor.fetchmany(50)


# Retry rate limit errors for 5 minutes
@backoff.on_predicate(
    backoff.expo,
    lambda resp: resp.status_code == 429,
    max_time=300
)
# Retry on server error 3 times
@backoff.on_predicate(
    backoff.expo,
    lambda resp: resp.status_code >= 500,
    max_tries=3
)
def unsubscribe_emails_braze(braze_api_server, braze_api_key, emails):
    if len(emails) > 50:
        raise ValueError(
            f"At most 50 emails at a time can be unsubscribed "
            f"to Braze, attempted {len(emails)}"
        )

    return requests.post(
        url=f"{braze_api_server}/email/status",
        json={
            'email': emails,
            'subscription_state': 'unsubscribed',
        },
        headers={'Authorization': f'BEARER {braze_api_key}'}
    )


@task
@backoff.on_exception(backoff.expo,
                      ProgrammingError,
                      max_tries=3)
def sync_braze_to_sailthru(
    sf_credentials: snowflake.SFCredentials,
    sf_role: str,
    sf_database: str,
    sf_schema: str,
    sailthru_api_key: str,
    sailthru_api_secret: str,
):
    logger = prefect.context.get("logger")
    sailthru_client = SailthruClient(sailthru_api_key, sailthru_api_secret)
    sf_connection = snowflake.create_snowflake_connection(
        sf_credentials,
        sf_role,
    )
    sf_table = 'email_unsubscribes_braze_to_sailthru'
    query = """
    SELECT email FROM {table}
    """.format(
        table=snowflake.qualified_table_name(sf_database, sf_schema, sf_table),
    )
    cursor = sf_connection.cursor()
    cursor.execute(query)
    counter = 0

    for row in cursor:
        counter += 1
        unsubscribe_email_sailthru(sailthru_client, row[0])
        if counter % 500 == 0:
            logger.info("%s users processed.", counter)


@backoff.on_exception(backoff.expo, SailthruClientError, max_tries=5)
@backoff.on_predicate(backoff.expo, lambda resp: not resp.is_ok(), max_tries=5)
# Retry rate limit errors for 5 minutes
@backoff.on_predicate(
    backoff.expo,
    lambda resp: resp.get_rate_limit_headers()['remaining'] == 0,
    max_time=300
)
def unsubscribe_email_sailthru(sailthru_client, email):
    logger = prefect.context.get("logger")
    logger.debug("About to unsubscribe user %s from sailthru", email)
    return sailthru_client.save_user(email, {'optout_email': 'all'})
