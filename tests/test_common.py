#!/usr/bin/env python

"""
Tests for BigQuery utils in the `edx_prefectutils` package.
"""

from prefect.core import Flow

from edx_prefectutils import common


def test_generate_dates():
    with Flow("test") as f:
        task = common.generate_dates(
            start_date='2020-01-01',
            end_date='2020-01-05'
        )
    state = f.run()
    assert state.is_successful()
    assert state.result[task].result == ['20200101', '20200102', '20200103', '20200104']


def test_get_unzipped_cartesian_product():
    with Flow("test") as f:
        task = common.get_unzipped_cartesian_product(
            input_lists=[[1, 2, 3], ["a", "b", "c"]]
        )
    state = f.run()
    assert state.is_successful()
    assert state.result[task].result == [
      (1, 1, 1, 2, 2, 2, 3, 3, 3),
      ("a", "b", "c", "a", "b", "c", "a", "b", "c")
    ]
