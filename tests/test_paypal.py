"""
Tests for paypal SFTP.
"""

import datetime

from mock import Mock

from edx_prefectutils.paypal import get_paypal_filename


def test_get_paypal_filename():
    connection_mock = Mock()
    mock_list = [
        'TRR-20201209.01.009_TEST.CSV', 'TRR-20201209.01.009.CSV', 'TRR-20210114.01.009.CSV', 'TRR-20210115.01.011.CSV'
    ]
    connection_mock.listdir = Mock(return_value=mock_list)

    filename = get_paypal_filename(datetime.date(2020, 12, 9), 'TRR', connection_mock, 'dummy')
    assert filename == 'TRR-20201209.01.009.CSV'

    filename = get_paypal_filename(datetime.date(2021, 1, 15), 'TRR', connection_mock, 'dummy')
    assert filename == 'TRR-20210115.01.011.CSV'
