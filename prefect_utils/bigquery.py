"""
Utility methods and tasks for working with BigQuery/Google Cloud Storage from a Prefect flow.
"""

from prefect import task
from prefect.utilities.gcp import get_storage_client
from urllib.parse import urlparse


@task
def cleanup_gcs_files(gcp_credentials: dict, url: str, project: str):
    """
    Task to delete files from a GCS prefix.

    Arguments:
      gcp_credentials (dict): GCP credentials in a format required by prefect.utilities.gcp.get_storage_client.
      url (str): Pointer to a GCS prefix containing one or more objects to delete.
      project (str): Name of the project which contains the target objects.
    """
    gcs_client = get_storage_client(credentials=gcp_credentials, project=project)
    parsed_url = urlparse(url)
    bucket = gcs_client.get_bucket(parsed_url.netloc)
    prefix = parsed_url.path.lstrip("/")
    blobs = bucket.list_blobs(prefix=prefix)
    bucket.delete_blobs(blobs)
