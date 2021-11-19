"""
Test for S3 related tasks
"""

from mock import patch
from mock.mock import MagicMock

from edx_prefectutils.s3 import delete_s3_directory


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
