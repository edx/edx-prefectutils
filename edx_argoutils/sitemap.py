"""
Functions for pulling sitemap data.
"""

import json
import xml.etree.ElementTree as ET
from os.path import basename, join, splitext
from urllib.parse import urlparse
import boto3
import requests
from .common import get_date


def fetch_sitemap_urls(sitemap_index_url: str) -> str:
    """
    Fetches a list of sitemap urls by parsing sitemap index file.
    """
    r = requests.get(sitemap_index_url)
    sitemap_index_xml = r.text
    tree = ET.fromstring(sitemap_index_xml)
    sitemap_urls = [sitemap_node.find('{*}loc').text.strip() for sitemap_node in tree.findall('{*}sitemap')]
    return sitemap_urls


def fetch_sitemap(sitemap_url: str):
    """
    Fetches sitemap data from a given sitemap URL.
    """
    scraped_at = get_date(None)
    r = requests.get(sitemap_url)
    sitemap_xml = r.text
    tree = ET.fromstring(sitemap_xml)

    sitemap_json = [
        {'scraped_at': scraped_at, 'url': url_node.find('{*}loc').text.strip()}
        for url_node in tree.findall('{*}url')
    ]

    sitemap_filename, ext = splitext(basename(urlparse(sitemap_url).path))
    return sitemap_filename, json.dumps(sitemap_json)


def write_sitemap_to_s3(sitemap_data: str, s3_bucket: str, s3_path: str, credentials: dict = {}):
    """
    Writes sitemap data in JSON format to S3.
    """
    filename, sitemap_json = sitemap_data
    today = get_date(None)
    date_path = f'{today}/{filename}.json'
    s3_key = f'{s3_path}{date_path}'
    s3_client = boto3.client(
        's3',
        aws_access_key_id=credentials.get('AccessKeyId'),
        aws_secret_access_key=credentials.get('SecretAccessKey'),
        aws_session_token=credentials.get('SessionToken')
    )
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=sitemap_json,
        ContentType='application/json'
    )

    return date_path
