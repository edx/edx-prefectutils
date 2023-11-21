"""
S3 related common methods and tasks for Prefect
"""

# import prefect
import logging
# from prefect import task
# from prefect.tasks.aws import s3
from airflow.hooks.S3_hook import S3Hook
import boto3
from botocore.client import Config
from prefect.utilities.aws import get_boto_client


# @task
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
        # logger = prefect.context.get("logger")
        logger = logging.getLogger()
        logger.info("Deleting S3 keys: {}".format(s3_keys))
        s3_client.delete_objects(
            Bucket=bucket,
            Delete={
                'Objects': [{'Key': key} for key in s3_keys]
            }
        )


# @task
def delete_objects_from_s3(objects_to_delete: list, bucket: str = None, credentials: dict = None, s3_conn_id: str ):
    """
    Delete an object or objects from S3.

    key (str): Name of the object within the S3 bucket (/foo/bar/baz.json) (No Longer using because the delete objects function does not take a single file path)
    objects_to_delete (list of dicts): List of objects to be deleted with the format [{'Key': 'path/to/your/object1.txt'}, {'Key': 'path/to/your/object2.txt'}]
    bucket (str): Name of the S3 bucket to delete from.
    credentials (dict): AWS credentials, if None boto will fall back the usual methods of resolution.
    """
    # s3_client = get_boto_client("s3", credentials=credentials)
    # s3_client.delete_object(Bucket=bucket, Key=key)

    s3_hook = S3Hook(s3_conn_id)
    s3_hook.delete_objects(bucket_name=bucket, objects=objects_to_delete )



# @task
def list_object_keys_from_s3(bucket: str = None, prefix: str = '', credentials: dict = None, s3_conn_id: str):
    """
    List objects key names from an S3 bucket that match the given prefix.

    prefix (str): Prefix path to search (ex: /foo/bar will match /foo/bar/baz and /foo/bar/baz/bing ...)
    bucket (str): Name of the S3 bucket to search from.
    credentials (dict): AWS credentials, if None boto will fall back the usual methods of resolution.
    """
    # s3_client = get_boto_client("s3", credentials=credentials)
    # found_objects = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    s3_hook = S3Hook(s3_conn_id)
    found_objects = s3_hook.list_keys(bucket_name=s3_bucket_name, prefix=prefix)

    # logger = prefect.context.get("logger")
    logger = logging.getLogger()
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


# @task
def write_report_to_s3(download_results: tuple, s3_bucket: str, s3_path: str, s3_conn_id: str):
    # logger = prefect.context.get("logger")
    logger = logging.getLogger()

    date, report_str = download_results
    date_path = get_s3_path_for_date(date)
    s3_key = s3_path + date_path
    logger.info("Writing report to S3 for {} to {}".format(date, s3_key))

    s3_hook = S3Hook(s3_conn_id)

    s3_hook.load_file(
            file_name=report_str, key=s3_key, bucket_name=s3_bucket
        )

    # s3.S3Upload(bucket=s3_bucket).run(
    #     report_str,
    #     key=s3_key
    # )

    return date_path


# @task
def get_s3_url(s3_bucket, s3_path):
    return 's3://{bucket}/{path}'.format(bucket=s3_bucket, path=s3_path)
