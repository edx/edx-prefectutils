"""
S3 related common methods and tasks for Prefect
"""

import prefect
from prefect import task
from prefect.utilities.aws import get_boto_client


@task
def delete_object_from_s3(key: str = None, bucket: str = None, credentials: dict = None, ):
    s3_client = get_boto_client("s3", credentials=credentials)
    s3_client.delete_object(Bucket=bucket, Key=key)


@task
def list_object_keys_from_s3(bucket: str = None, prefix: str = '', credentials: dict = None, ):
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
