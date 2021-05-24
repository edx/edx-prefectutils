import backoff
import prefect
import requests
from prefect import task
from prefect.backend import get_key_value, set_key_value
from snowflake.connector import ProgrammingError

from .snowflake import SFCredentials, get_batched_rows_from_snowflake

# Attribute arrays can only hold 25 items, though we could ask for this limit to be bumped. As of the time of this
# writing, our longest leads list is 14 items long (and only 4 users have more than 10). I think we're OK for now.
# https://www.braze.com/docs/api/objects_filters/user_attributes_object/
ARRAY_LIMIT = 25

# https://www.braze.com/docs/api/endpoints/user_data/post_user_track/
ATTRIBUTES_LIMIT = 75


def _make_custom_user_attrs(row):
    """Dict of attributes that are owned by this script"""
    # Masters are separated by semicolons in the snowflake database
    masters_leads = row.masters_leads.split(';') if row.masters_leads else []
    if len(masters_leads) > ARRAY_LIMIT:
        raise ValueError(
            f'At most {ARRAY_LIMIT} masters leads at a time can be set '
            f'in Braze, but we have {len(masters_leads)} for hubspot user {row.hs_object_id}'
        )

    # Programs are separated by semicolons in the snowflake database
    braze_programs = row.braze_programs.split(';') if row.braze_programs else []
    if len(braze_programs) > ARRAY_LIMIT:
        raise ValueError(
            f'At most {ARRAY_LIMIT} program interests at a time can be set '
            f'in Braze, but we have {len(braze_programs)} for hubspot user {row.hs_object_id}'
        )

    # Yes these are inconsistent. But it's a matter of historical field names.
    return {
        'HubSpot Audience': row.audience,
        'HubSpot Contact': row.braze_hs_contact,
        'HubSpot Contact ID': row.hs_object_id,
        'HubSpot Create Date': row.createdate.isoformat(),
        'HubSpot - First Conversion': row.first_conversion_event_name,
        'HubSpot - Lead Source': row.lead_source_deal_value,
        'HubSpot - Lead Source Detail': row.lead_source,  # yes, the bare `lead_source` is actually the detail
        'HubSpot Level of Education': row.highest_level_of_education,
        'HubSpot Masters Interests': masters_leads,
        'HubSpot Program Interests': braze_programs,
        'HubSpot - utm_campaign': row.utm_campaign,
        'HubSpot - utm_medium': row.utm_medium,
        'HubSpot - utm_source': row.utm_source,
    }


def _make_standard_user_attrs(row):
    """Dict of attributes that are standard braze ones - that we likely only need to set for unregistered users"""
    return {
        'country': row.country_iso_code,
        'email': row.email,
        'first_name': row.firstname,
        'home_city': row.city,
        'last_name': row.lastname,
        'phone': row.phone,
    }


@task
@backoff.on_exception(backoff.expo,
                      ProgrammingError,
                      max_tries=3)
def sync_hubspot_leads_to_braze(
    sf_credentials: SFCredentials,
    sf_role: str,
    sf_database: str,
    sf_schema: str,
    braze_api_key: str,
    braze_api_server: str,
):
    """
    Copies a bunch of hubspot data fields over to matching braze users.

    Note that we must handle users that don't have a proper braze external_id, because they are not registered.
    """
    logger = prefect.context.get('logger')
    columns = [
        'audience',
        'braze_hs_contact',
        'braze_programs',
        'city',
        'country_iso_code',
        'createdate',
        'email',
        'first_conversion_event_name',
        'firstname',
        'highest_level_of_education',
        'hs_object_id',
        'lastname',
        'lastmodifieddate',
        'lead_source',
        'lead_source_deal_value',
        'lms_user_id',
        'masters_leads',
        'phone',
        'utm_campaign',
        'utm_medium',
        'utm_source',
    ]

    # There are two types of users:
    # - Hubspot-alias-only users that don't have an account on the LMS
    # - Users that have an lms_user_id (maybe never had hubspot, maybe did - 'identifying' aliases is done by the
    #   platform-plugin-braze plugin that calls Braze's 'users/identify' API as users register on the LMS)
    #
    # We only want to set the "basic" fields like name/phone/email for alias-only users. Registered users get those
    # fields from the LMS. But both categories get all the hubspot-specific fields.

    alias_only_where = 'lms_user_id IS NULL'
    registered_where = 'lms_user_id IS NOT NULL'

    # Also, to reduce churn and data usage, only sync items that have changed since our last successful run
    LAST_SUCCESS_KEY = 'hubspot-leads-last-success'
    try:
        last_success = get_key_value(key=LAST_SUCCESS_KEY)
        alias_only_where += f" AND lastmodifieddate > '{last_success}'"
        registered_where += f" AND lastmodifieddate > '{last_success}'"
        logger.info('Read lastmodifieddate of %s', last_success)
    except ValueError:
        pass

    def _get_batches(label, where=None):
        count = 0
        for rows in get_batched_rows_from_snowflake(sf_credentials, sf_database, sf_schema, 'hubspot_leads', sf_role,
                                                    columns, ATTRIBUTES_LIMIT, where=where):
            count += len(rows)
            yield rows
            if count % (ATTRIBUTES_LIMIT * 10) == 0:
                logger.info(label, count)
        if count > 0:
            logger.info(label, count)

    max_date = None

    for batch in _get_batches('Updated %d hubspot alias users in Braze', where=alias_only_where):
        update_alias_users(braze_api_server, braze_api_key, batch)
        max_batch_date = max(row.lastmodifieddate for row in batch)
        max_date = max(max_batch_date, max_date) if max_date else max_batch_date

    for batch in _get_batches('Updated %d registered users in Braze', where=registered_where):
        update_registered_users(braze_api_server, braze_api_key, batch)
        max_batch_date = max(row.lastmodifieddate for row in batch)
        max_date = max(max_batch_date, max_date) if max_date else max_batch_date

    if max_date:
        logger.info('Saving lastmodifieddate of %s', max_date.isoformat())
        set_key_value(key=LAST_SUCCESS_KEY, value=max_date.isoformat())


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
def update_registered_users(braze_api_server, braze_api_key, rows):
    """
    Sets custom braze attributes on a list of registered users (users with an external_id set).

    Args:
        braze_api_server (str)
        braze_api_key (str)
        rows (list): sequence of user info
    """
    if len(rows) > ATTRIBUTES_LIMIT:
        raise ValueError(
            f'At most {ATTRIBUTES_LIMIT} users at a time can be updated in Braze, but we attempted {len(rows)}'
        )

    return requests.post(
        url=f"{braze_api_server}/users/track",
        headers={'Authorization': f'Bearer {braze_api_key}'},
        json={
            'attributes': [
                # https://www.braze.com/docs/api/objects_filters/user_attributes_object/
                {
                    'external_id': row.lms_user_id,
                    **_make_custom_user_attrs(row),
                } for row in rows
            ],
        },
    )


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
def update_alias_users(braze_api_server, braze_api_key, rows):
    """
    Update attributes for the given hubspot-alias-only users in braze, which don't have an external_id set.

    Args:
        braze_api_server (str)
        braze_api_key (str)
        rows (list): sequence of user info
    """
    if len(rows) > ATTRIBUTES_LIMIT:
        raise ValueError(
            f'At most {ATTRIBUTES_LIMIT} users at a time can be updated in Braze, but we attempted {len(rows)}'
        )

    return requests.post(
        url=f'{braze_api_server}/users/track',
        headers={'Authorization': f'Bearer {braze_api_key}'},
        json={
            'attributes': [
                # https://www.braze.com/docs/api/objects_filters/user_attributes_object/
                {
                    '_update_existing_only': False,
                    'user_alias': {
                        'alias_label': 'hubspot',
                        'alias_name': row.email,
                    },
                    **_make_standard_user_attrs(row),
                    **_make_custom_user_attrs(row),
                } for row in rows
            ],
        },
    )
