"""
Utility methods and tasks for use from a Prefect flow.
"""

import datetime
import itertools

import prefect
from prefect import task


@task
def generate_dates(start_date: str, end_date: str):
    """
    Generates a list of date strings in the format `YYYYMMDD` from start_date up to but excluding end_date.
    """
    if not start_date:
        start_date = prefect.context.yesterday
    if not end_date:
        end_date = prefect.context.today

    parsed_start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    parsed_end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    dates = []
    while parsed_start_date < parsed_end_date:
        dates.append(parsed_start_date)
        parsed_start_date = parsed_start_date + datetime.timedelta(days=1)

    return [date.strftime("%Y%m%d") for date in dates]


@task
def get_unzipped_cartesian_product(input_lists: list):
    """
    Generate an unzipped cartesian product of the given list of lists, useful for
    generating task parameters for mapping.

    For example, get_unzipped_cartesian_product([[1, 2, 3], ["a", "b", "c"]]) would return:

    [
      [1, 1, 1, 2, 2, 3, 3, 3],
      ["a", "b", "c", "a", "b", "c", "a", "b", "c"]
    ]

    Args:
      input_lists (list): A list of two or more lists.
    """
    return list(zip(*itertools.product(*input_lists)))
