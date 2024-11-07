"""
Utility functions for use in Argo flows.
"""

import itertools
import re
import six
from opaque_keys import InvalidKeyError
from opaque_keys.edx.keys import CourseKey
from datetime import datetime, timedelta, date



def get_date(date: str):
    """
    Return today's date string if date is None. Otherwise return the passed parameter value.
    """
    if date is None:
        return datetime.today().strftime('%Y-%m-%d')
    else:
        return date


def generate_dates(start_date: str, end_date: str, date_format: str = "%Y%m%d"):
    """
    Generates a list of date strings in the format specified by `date_format` from
    start_date up to but excluding end_date.
    """
    if not start_date:
        start_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.today().strftime("%Y-%m-%d")

    parsed_start_date = datetime.strptime(start_date, "%Y-%m-%d")
    parsed_end_date = datetime.strptime(end_date, "%Y-%m-%d")
    dates = []
    while parsed_start_date < parsed_end_date:
        dates.append(parsed_start_date)
        parsed_start_date = parsed_start_date + timedelta(days=1)

    return [date.strftime(date_format) for date in dates]


def generate_month_start_dates(start_date: str, end_date: str, date_format: str = "%Y-%m-%d"):
    """
    Return a list of first days of months within the specified date range.
    If start_date or end_date is not provided, defaults to yesterday or today respectively.
    """
    if not start_date:
        start_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.today().strftime("%Y-%m-%d")

    # Since our intention is to extract first day of months, we will start by modifying the start and end date
    # to represent the first day of month.
    parsed_start_date = datetime.strptime(start_date, "%Y-%m-%d").replace(day=1)
    parsed_end_date = datetime.strptime(end_date, "%Y-%m-%d").replace(day=1)
    dates = []
    current_date = parsed_start_date
    while current_date <= parsed_end_date:
        dates.append(current_date)
        # The addition of 32 days to current_date and then setting the day to 1 is a way to ensure that we move to
        # the beginning of the next month, even if the month doesn't have exactly 32 days.
        current_date += timedelta(days=32)
        current_date = current_date.replace(day=1)

    return [date.strftime(date_format) for date in dates]


def get_unzipped_cartesian_product(input_lists: list):
    """
    Generate an unzipped cartesian product of the given list of lists.

    For example, get_unzipped_cartesian_product([[1, 2, 3], ["a", "b", "c"]]) would return:

    [
      [1, 1, 1, 2, 2, 3, 3, 3],
      ["a", "b", "c", "a", "b", "c", "a", "b", "c"]
    ]

    Args:
      input_lists (list): A list of two or more lists.
    """
    return list(zip(*itertools.product(*input_lists)))


def get_filename_safe_course_id(course_id, replacement_char='_'):
    """
    Create a representation of a course_id that can be used safely in a filepath.
    """
    try:
        course_key = CourseKey.from_string(course_id)
        # Ignore the namespace of the course_id altogether, for backwards compatibility.
        filename = course_key._to_string()  # pylint: disable=protected-access
    except InvalidKeyError:
        # If the course_id doesn't parse, we will still return a value here.
        filename = course_id

    # The safest characters are A-Z, a-z, 0-9, <underscore>, <period> and <hyphen>.
    # We represent the first four with \w.
    # TODO: Once we support courses with unicode characters, we will need to revisit this.
    return re.sub(r'[^\w\.\-]', six.text_type(replacement_char), filename)

def generate_date_range(start_date= None, end_date= None, is_daily: bool = None):
    """
    Generate a list of dates depending on parameters passed. Dates are inclusive.
        Custom dates is top priority: start_date & end_date are set, is_daily = False
        Daily run: start_date & end_date are both None, is_daily = True
        True-up run: start_date & end_date are both None, is_daily = False

    Args:
        start_date (str): The start date in YYYY-MM-DD format
        end_date (str): The end date in YYYY-MM-DD format
        is_daily (bool): Designates if last completed day to run
    """

    if start_date is not None and end_date is not None and is_daily is False:
        # Manual run: user entered parameters for custom dates
        #logger.info("Setting dates for manual run...")
        #start_date= start_date.strftime('%Y-%m-%d')
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        #end_date= end_date.strftime('%Y-%m-%d')

    elif start_date is None and end_date is None and is_daily is True:
        # Daily run: minus 2 lag completed day, eg. if today is 9/14, output is 9/12
        # this is due to timing issues with data freshness for STRIPE_RAW <> Snowflake share
        minus_two_lag_date = datetime.strptime(str(date.today()), "%Y-%m-%d").date() - timedelta(days=2)
        start_date, end_date = minus_two_lag_date, minus_two_lag_date

    elif start_date is None and end_date is None and is_daily is False:
        # True-up: Calculate the last completed month
        today = datetime.now().date()
        current_month_start = datetime(today.year, today.month, 1).date()
        last_month_end = current_month_start - timedelta(days=1)
        last_month_start = datetime(last_month_end.year, last_month_end.month, 1).date()

        start_date = last_month_start
        end_date = last_month_end

    else:
        raise Exception("Incorrect parameters passed!")

    current_date = start_date
    date_list = []

    while current_date <= end_date:
        date_list.append(current_date)
        current_date += timedelta(days=1)

    return date_list
