"""
Test for S3 related tasks
"""

import csv
import os
import tempfile
from unittest import TestCase

from mock import patch
from mock.mock import MagicMock
from moto import mock_s3

from edx_prefectutils.s3 import (delete_s3_directory, get_boto_client,
                                 get_s3_csv_column_names)


@patch("edx_prefectutils.s3.list_object_keys_from_s3.run")
@patch("edx_prefectutils.s3.get_boto_client")
def test_delete_s3_directory(boto_client_mock, list_object_keys_from_s3_mock):
    """
    Test the delete_s3_directory task
    """
    keys = ["some/prefix/data_0_0_0.csv", "some/prefix/data_0_0_1.csv", "some/prefix/data_0_0_2.csv"]
    list_object_keys_from_s3_mock.return_value = keys
    client_mock = MagicMock()
    boto_client_mock.return_value = client_mock
    task = delete_s3_directory
    task.run(bucket="bucket", prefix="some/prefix/")

    client_mock.delete_objects.assert_called_once_with(
        Bucket="bucket",
        Delete={
            "Objects": [{"Key": key} for key in keys]
        },
    )


class S3CSVHeaderTest(TestCase):

    def setUp(self):
        self.mock_s3 = mock_s3()
        self.mock_s3.start()

        self.bucket = 'ma_test_bucket'
        self.prefix = 'reports/progress/'
        self.s3_url = f's3://{self.bucket}/{self.prefix}'
        s3_client = get_boto_client("s3")
        s3_client.create_bucket(Bucket=self.bucket)
        csv_file_path = self.create_input_data_csv()
        # create an empty object which is equivalent to creating a folder in bucket
        s3_client.put_object(Bucket=self.bucket, Body='', Key=self.prefix)
        s3_client.upload_file(csv_file_path, self.bucket, f"{self.prefix}test_data.csv")

    def tearDown(self):
        self.mock_s3.stop()

    def create_input_data_csv(self):
        """Create csv with fake date"""
        tmp_csv_path = os.path.join(tempfile.gettempdir(), 'data.csv')

        with open(tmp_csv_path, 'w') as csv_file:  # pylint: disable=unspecified-encoding
            csv_writer = csv.DictWriter(csv_file, fieldnames=['id', 'first', 'last', 'email'])
            csv_writer.writeheader()
            csv_writer.writerows([
                {'id': 1, 'first': 'Bruce', 'last': 'Wayne', 'email': 'bruce@example.com'},
                {'id': 2, 'first': 'Barry', 'last': 'Allen', 'email': 'barry@example.com'},
                {'id': 3, 'first': 'Kent', 'last': 'Clark', 'email': 'kent@example.com'},
            ])

        return tmp_csv_path

    def test_get_s3_csv_column_names(self):
        """
        Verify the `get_s3_csv_column_names` works as expected.
        """
        csv_column_names = get_s3_csv_column_names.run(self.s3_url)
        assert csv_column_names == ['id', 'first', 'last', 'email']
