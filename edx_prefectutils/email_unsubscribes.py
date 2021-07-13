
import backoff
import prefect
import requests
from prefect import task
from prefect.backend import get_key_value, set_key_value
from snowflake.connector import ProgrammingError

from . import snowflake


@task
@backoff.on_exception(backoff.expo,
                      ProgrammingError,
                      max_tries=3)
def sync_hubspot_to_braze(
    sf_credentials: snowflake.SFCredentials,
    sf_role: str,
    sf_database: str,
    sf_schema: str,
    braze_api_key: str,
    braze_api_server: str,
):
    logger = prefect.context.get("logger")

    # Also, to reduce churn and data usage, only sync items that have changed since our last successful run
    LASTMODIFIEDDATE_KEY = 'email-unsubscribes-hubspot-lastmodifieddate'
    where = ''
    try:
        lastmodifieddate = get_key_value(key=LASTMODIFIEDDATE_KEY)
        where = f" WHERE lastmodifieddate > '{lastmodifieddate}'"
        logger.info('Read lastmodifieddate of %s', lastmodifieddate)
    except ValueError:
        pass

    sf_connection = snowflake.create_snowflake_connection(
        sf_credentials,
        sf_role,
    )
    sf_table = 'email_unsubscribes_hubspot_to_braze'
    query = """
    SELECT email, lastmodifieddate FROM {table} {where}
    """.format(
        table=snowflake.qualified_table_name(sf_database, sf_schema, sf_table),
        where=where,
    )
    cursor = sf_connection.cursor()
    cursor.execute(query)
    count = 0
    max_date = None
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

        max_batch_date = max(row[1] for row in batch)
        max_date = max(max_batch_date, max_date) if max_date else max_batch_date

        batch = cursor.fetchmany(50)

    if max_date:
        logger.info('Saving lastmodifieddate of %s', max_date.isoformat())
        set_key_value(key=LASTMODIFIEDDATE_KEY, value=max_date.isoformat())


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
