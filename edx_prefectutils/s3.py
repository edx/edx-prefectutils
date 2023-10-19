"""
S3 related common methods and tasks for Prefect
"""

import csv
import io
from urllib.parse import urlparse

import prefect
from prefect import task
from prefect.tasks.aws import s3
from prefect.utilities.aws import get_boto_client


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


def get_s3_path_for_date(date):
    # The path and file name inside our given bucket and S3 prefix to write the file to
    return '{date}/{date}.json'.format(date=date)


@task
def write_report_to_s3(download_results: tuple, s3_bucket: str, s3_path: str):
    logger = prefect.context.get("logger")

    date, report_str = download_results
    date_path = get_s3_path_for_date(date)
    s3_key = s3_path + date_path
    logger.info("Writing report to S3 for {} to {}".format(date, s3_key))

    s3.S3Upload(bucket=s3_bucket).run(
        report_str,
        key=s3_key
    )

    return date_path


@task
def get_s3_url(s3_bucket, s3_path):
    return 's3://{bucket}/{path}'.format(bucket=s3_bucket, path=s3_path)


def parse_s3_url(s3_url):
    """
    Parse and return bucket name and key
    """
    parsed = urlparse(s3_url)
    bucket = parsed.netloc
    # remove slash from the start of the key
    key = parsed.path.lstrip('/')
    return bucket, key


@task
def get_s3_csv_column_names(s3_url):
    """
    Read a csv file in S3 and return its header.
    """
    logger = prefect.context.get("logger")

    bucket, key = parse_s3_url(s3_url)
    s3_client = get_boto_client("s3")
    objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)

    header = []
    if objects['KeyCount']:
        # find the key of first object that is actually a csv
        first_csv_object_key = next((obj['Key'] for obj in objects['Contents'] if obj['Key'].endswith(".csv")), None)
        response = s3_client.get_object(Bucket=bucket, Key=first_csv_object_key)
        reader = csv.reader(io.TextIOWrapper(response['Body'], encoding="utf-8"))
        header = next(reader)
        logger.info('CSV: [{}], Header: [{}]'.format(first_csv_object_key, header))

    return header
