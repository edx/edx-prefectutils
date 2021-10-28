"""
Tasks for pulling sitemap data.
"""

import json
import xml.etree.ElementTree as ET
from os.path import basename, join, splitext
from urllib.parse import urlparse

import prefect
import requests
from prefect import task
from prefect.tasks.aws import s3


@task
def fetch_sitemap_urls(sitemap_index_url: str) -> str:
    """
    Fetches a list of sitemap urls by parsing sitemap index file.
    """
    r = requests.get(sitemap_index_url)
    sitemap_index_xml = r.text
    tree = ET.fromstring(sitemap_index_xml)
    sitemap_urls = [sitemap_node.find('{*}loc').text.strip() for sitemap_node in tree.findall('{*}sitemap')]
    return sitemap_urls


@task
def fetch_sitemap(sitemap_url: str):
    """
    Fetches sitemap data from a given sitemap URL.
    """
    scraped_at = str(prefect.context.date)
    r = requests.get(sitemap_url)
    sitemap_xml = r.text
    tree = ET.fromstring(sitemap_xml)

    sitemap_json = [
        {'scraped_at': scraped_at, 'url': url_node.find('{*}loc').text.strip()}
        for url_node in tree.findall('{*}url')
    ]

    sitemap_filename, ext = splitext(basename(urlparse(sitemap_url).path))
    return sitemap_filename, json.dumps(sitemap_json)


@task
def write_sitemap_to_s3(sitemap_data: str, s3_bucket: str, s3_path: str):
    """
    Writes sitemap data in JSON format to S3.
    """
    filename, sitemap_json = sitemap_data
    date_path = f'{prefect.context.today}/{filename}.json'
    s3_key = join(s3_path, date_path)
    s3.S3Upload(bucket=s3_bucket).run(
        sitemap_json,
        key=s3_key,
    )

    return date_path
