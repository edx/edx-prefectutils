"""
S3 related common methods
"""

import boto3
import logging

logger = logging.getLogger("s3")

def get_s3_client(credentials: dict = None):
    s3_client = None
    if credentials:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=credentials.get('AccessKeyId'),
            aws_secret_access_key=credentials.get('SecretAccessKey'),
            aws_session_token=credentials.get('SessionToken')
        )
    else:
        s3_client = boto3.client('s3')

    return s3_client

def delete_s3_directory(bucket: str = None, prefix: str = None, credentials: dict = None):
    """
    Deletes all objects with the given S3 directory (prefix) from the given bucket.

    Args:
        bucket (str): The S3 bucket to delete the objects from.
        prefix (str): The S3 prefix to delete the objects from.
        credentials (dict): The AWS credentials to use.
    """
    s3_keys = list_object_keys_from_s3(bucket, prefix, credentials)
    if s3_keys:
        s3_client = get_s3_client(credentials)
        logger.info("Deleting S3 keys: {}".format(s3_keys))
        s3_client.delete_objects(
            Bucket=bucket,
            Delete={
                'Objects': [{'Key': key} for key in s3_keys]
            }
        )


def delete_object_from_s3(key: str = None, bucket: str = None, credentials: dict = None, ):
    """
    Delete an object from S3.

    key (str): Name of the object within the S3 bucket (/foo/bar/baz.json)
    bucket (str): Name of the S3 bucket to delete from.
    credentials (dict): AWS credentials, if None boto will fall back the usual methods of resolution.
    """
    s3_client = get_s3_client(credentials)
    s3_client.delete_object(Bucket=bucket, Key=key)


def list_object_keys_from_s3(bucket: str = None, prefix: str = '', credentials: dict = None, ):
    """
    List objects key names from an S3 bucket that match the given prefix.

    prefix (str): Prefix path to search (ex: /foo/bar will match /foo/bar/baz and /foo/bar/baz/bing ...)
    bucket (str): Name of the S3 bucket to search from.
    credentials (dict): AWS credentials, if None boto will fall back the usual methods of resolution.
    """
    s3_client = get_s3_client(credentials)
    

    found_objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    logger.info(f"Found objects: {found_objects}")
    # edx_legacy/segment-config/dev/load_segment_config_to_snowflake/2024-11-08/2024-11-08.json

    if found_objects['KeyCount']:
        return [
            o['Key'] for o in found_objects['Contents']
        ]
    else:
        return []


def get_s3_path_for_date(filename):
    # The path and file name inside our given bucket and S3 prefix to write the file to
    return '{filename}/{filename}.json'.format(filename=filename)


def write_report_to_s3(download_results: tuple, s3_bucket: str, s3_path: str, credentials: dict = None):
    filename, report_str = download_results
    file_path = get_s3_path_for_date(filename)
    s3_key = s3_path + file_path
    logger.info("Writing report to S3 for {} to {}".format(filename, s3_key))

    s3_client = get_s3_client(credentials)

    s3_client.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=report_str,
        ContentType='application/json'
    )
    return file_path


def get_s3_url(s3_bucket, s3_path):
    return 's3://{bucket}/{path}'.format(bucket=s3_bucket, path=s3_path)
