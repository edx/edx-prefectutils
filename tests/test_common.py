#!/usr/bin/env python

"""
Tests for Common utils in the `edx_argoutils`.
"""

from edx_argoutils import common
from datetime import datetime, date
from unittest.mock import patch
from opaque_keys.edx.keys import CourseKey

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
    

def test_valid_course_id():
    course_id = "course-v1:BerkeleyX+CS198.SDC.1+1T2021"
    result = "BerkeleyX_CS198.SDC.1_1T2021" 
    with patch.object(CourseKey, 'from_string') as mock_from_string:
        mock_from_string.return_value._to_string.return_value = result
        assert result == common.get_filename_safe_course_id(course_id)

def test_invalid_course_id():
    course_id = "BerkeleyX!CS198.SDC.1!1T2021"
    result = "BerkeleyX_CS198.SDC.1_1T2021"
    assert result == common.get_filename_safe_course_id(course_id) 

def test_generate_date_range():
    # Test Case 1: Custom date range (is_daily=False)
    result = common.generate_date_range(
        start_date='2025-01-01',
        end_date='2025-01-05',
        is_daily=False
    )
    assert result == [
        datetime.strptime(date, '%Y-%m-%d').date()
        for date in ['2025-01-01', '2025-01-02', '2025-01-03', '2025-01-04', '2025-01-05']
    ]

    # Test Case 2: Daily run (is_daily=True) with mock
    fixed_today = date(2025, 1, 28)  # Assume today's date is 2025-01-28
    with patch('edx_argoutils.common.date') as mock_date:
        mock_date.today.return_value = fixed_today
        mock_date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)

        result = common.generate_date_range(is_daily=True)
        expected = [date(2025, 1, 26)]  # Two days before fixed_today
        assert result == expected, f"Expected {expected}, but got {result}"

    #Test Case 3: True-up scenario (is_daily=False, no start_date and end_date)
    with patch('edx_argoutils.common.date') as mock_date:
        mock_date.today.return_value = fixed_today
        mock_date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)

        result = common.generate_date_range(is_daily=False)
        expected = [date(2024, 12, d) for d in range(1, 32)]  #Last completed month
        assert result == expected, f"Expected {expected}, but got {result}"

    # Test Case 4: Invalid parameters
    try:
        common.generate_date_range(
            start_date="2025-01-01",
            end_date=None,
            is_daily=False
        )
    except Exception as e:
        assert str(e) == "Incorrect parameters passed!"
    else:
        assert False, "Expected an exception but none was raised!"