#!/usr/bin/env python

"""
Tests for Common utils in the `edx_argoutils`.
"""

from edx_prefectutils import common


def test_generate_dates():
    result = common.generate_dates(
        start_date='2020-01-01',
        end_date='2020-01-05'
    )
    assert result == ['20200101', '20200102', '20200103', '20200104']


def test_generate_month_start_dates():
    result = common.generate_month_start_dates(
        start_date='2023-01-31',
        end_date='2023-05-05'
    )
    assert result == ['2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01', '2023-05-01']


def test_get_unzipped_cartesian_product():
    result = common.get_unzipped_cartesian_product(
        input_lists=[[1, 2, 3], ["a", "b", "c"]]
    )
    assert result == [
        (1, 1, 1, 2, 2, 2, 3, 3, 3),
        ("a", "b", "c", "a", "b", "c", "a", "b", "c")
    ]
