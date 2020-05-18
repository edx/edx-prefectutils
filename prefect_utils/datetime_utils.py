"""
Utility methods and tasks for working with dates and times from a Prefect flow.
"""

import datetime
from prefect import task


@task
def generate_dates(start_date: str, end_date: str):
    """
    Generates a list of date strings in the format `YYYYMMDD` from start_date up to but excluding end_date.
    """
    parsed_start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    parsed_end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    dates = []
    while parsed_start_date < parsed_end_date:
        dates.append(parsed_start_date)
        parsed_start_date = parsed_start_date + datetime.timedelta(days=1)

    return [date.strftime("%Y%m%d") for date in dates]
