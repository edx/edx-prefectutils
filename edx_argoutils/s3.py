"""
S3 related common methods and tasks for Prefect
"""

import prefect
from prefect import task
from prefect.tasks.aws import s3
from prefect.utilities.aws import get_boto_client
import boto3
import logging

@task
def delete_s3_directory(bucket: str = None, prefix: str = None, credentials: dict = None):
    """
    Deletes all objects with the given S3 directory (prefix) from the given bucket.

    Args:
        bucket (str): The S3 bucket to delete the objects from.
        prefix (str): The S3 prefix to delete the objects from.
        credentials (dict): The AWS credentials to use.
    """
    s3_keys = list_object_keys_from_s3.run(bucket, prefix, credentials)
    if s3_keys:
        s3_client = get_boto_client('s3', credentials=credentials)
        logger = prefect.context.get("logger")
        logger.info("Deleting S3 keys: {}".format(s3_keys))
        s3_client.delete_objects(
            Bucket=bucket,
            Delete={
                'Objects': [{'Key': key} for key in s3_keys]
            }
        )


@task
def delete_object_from_s3(key: str = None, bucket: str = None, credentials: dict = None, ):
    """
    Delete an object from S3.

    key (str): Name of the object within the S3 bucket (/foo/bar/baz.json)
    bucket (str): Name of the S3 bucket to delete from.
    credentials (dict): AWS credentials, if None boto will fall back the usual methods of resolution.
    """
    s3_client = get_boto_client("s3", credentials=credentials)
    s3_client.delete_object(Bucket=bucket, Key=key)


@task
def list_object_keys_from_s3(bucket: str = None, prefix: str = '', credentials: dict = None, ):
    """
    List objects key names from an S3 bucket that match the given prefix.

    prefix (str): Prefix path to search (ex: /foo/bar will match /foo/bar/baz and /foo/bar/baz/bing ...)
    bucket (str): Name of the S3 bucket to search from.
    credentials (dict): AWS credentials, if None boto will fall back the usual methods of resolution.
    """
    s3_client = get_boto_client("s3", credentials=credentials)
    found_objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    logger = prefect.context.get("logger")
    logger.info(found_objects)

    if found_objects['KeyCount']:
        return [
            o['Key'] for o in found_objects['Contents']
        ]
    else:
        return []


def get_s3_path_for_file(filename):
    # The path and file name inside our given bucket and S3 prefix to write the file to
    return '{filename}/{filename}.json'.format(filename=filename)


def write_report_to_s3(download_results: tuple, s3_bucket: str, s3_path: str, credentials: dict = None):
    logger = logging.getLogger("s3")

    filename, report_str = download_results
    file_path = get_s3_path_for_file(filename)
    s3_key = s3_path + file_path
    logger.info("Writing report to S3 for {} to {}".format(filename, s3_key))

    s3_client = boto3.client(
        's3',
        aws_access_key_id=credentials.get('AccessKeyId'),
        aws_secret_access_key=credentials.get('SecretAccessKey'),
        aws_session_token=credentials.get('SessionToken')
    )
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=report_str,
        ContentType='application/json'
    )
    return file_path


def get_s3_url(s3_bucket, s3_path):
    return 's3://{bucket}/{path}'.format(bucket=s3_bucket, path=s3_path)
