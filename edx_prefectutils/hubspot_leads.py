from collections import namedtuple

import backoff
import prefect
import requests
from prefect import task
from snowflake.connector import ProgrammingError

from . import snowflake

# masters_leads is an iterable of strings like "Master's in Data Science"
LeadsData = namedtuple('LeadsData', ['lms_user_id', 'masters_leads'])

# We can only update 75 attributes at a time:
# https://www.braze.com/docs/api/endpoints/user_data/post_user_track/
ATTRIBUTE_BATCH_SIZE = 75

# Attribute arrays can only hold 25 items, though we could ask for this limit to be bumped. As of the time of this
# writing, our longest leads list is 14 items long (and only 4 users have more than 10). I think we're OK for now.
# https://www.braze.com/docs/api/objects_filters/user_attributes_object/
MASTERS_LEADS_LIMIT = 25


@task
@backoff.on_exception(backoff.expo,
                      ProgrammingError,
                      max_tries=3)
def sync_hubspot_leads_to_braze(
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
    sf_table = 'hubspot_leads'
    query = """
    SELECT lms_user_id, masters_leads FROM {table} WHERE lms_user_id IS NOT NULL
    """.format(
        table=snowflake.qualified_table_name(sf_database, sf_schema, sf_table),
    )
    cursor = sf_connection.cursor()
    cursor.execute(query)
    count = 0
    batch = cursor.fetchmany(ATTRIBUTE_BATCH_SIZE)
    while len(batch) > 0:
        _send_hubspot_leads_to_braze_for_registered_users(
            logger,
            braze_api_server,
            braze_api_key,
            # Leads are separated by semicolons in the snowflake database
            [LeadsData(row[0], row[1].split(';')) for row in batch],
        )
        count += len(batch)
        if count % (ATTRIBUTE_BATCH_SIZE * 10) == 0:
            logger.info("Updated %d user attributes in Braze", count)
        batch = cursor.fetchmany(ATTRIBUTE_BATCH_SIZE)
    if count > 0:
        logger.info("Updated %d user attributes in Braze", count)


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
def _send_hubspot_leads_to_braze_for_registered_users(logger, braze_api_server, braze_api_key, leads_data):
    if len(leads_data) > ATTRIBUTE_BATCH_SIZE:
        raise ValueError(
            f"At most {ATTRIBUTE_BATCH_SIZE} user attributes at a time can be updated "
            f"in Braze, but we attempted {len(leads_data)}"
        )

    for data in leads_data:
        if len(data.masters_leads) > MASTERS_LEADS_LIMIT:
            raise ValueError(
                f"At most {MASTERS_LEADS_LIMIT} masters leads at a time can be set "
                f"in Braze, but we have {len(data.masters_leads)} for LMS user {data.lms_user_id}"
            )

    return requests.post(
        url=f"{braze_api_server}/users/track",
        headers={'Authorization': f'BEARER {braze_api_key}'},
        json={
            'attributes': [
                {
                    'external_id': data.lms_user_id,
                    # Custom attributes:
                    'masters_leads': data.masters_leads,
                } for data in leads_data
            ],
        },
    )
