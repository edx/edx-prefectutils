"""
Tests for sitemap tasks.
"""

import json

import requests
from mock import Mock, patch
from prefect import context

from edx_prefectutils.sitemap import fetch_sitemap, fetch_sitemap_urls

SCRAPED_AT = '2021-10-22T15:14:16.683985+00:00'


@patch.object(requests, 'get')
def test_fetch_sitemap_urls(mockget):
    mockresponse = Mock()
    mockget.return_value = mockresponse
    mockresponse.text = """<?xml version="1.0" encoding="UTF-8"?>
    <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
        <sitemap>
            <loc>https://www.foo.com/sitemap-0.xml</loc>
        </sitemap>
        <sitemap>
            <loc>https://www.foo.com/sitemap-1.xml</loc>
        </sitemap>
    </sitemapindex>
    """
    expected_output = ['https://www.foo.com/sitemap-0.xml', 'https://www.foo.com/sitemap-1.xml']
    task = fetch_sitemap_urls
    assert task.run(sitemap_index_url='dummy_url') == expected_output


@patch.object(requests, 'get')
def test_fetch_sitemap(mockget):
    mockresponse = Mock()
    mockget.return_value = mockresponse
    mockresponse.text = """<?xml version="1.0" encoding="UTF-8"?>
    <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:video="http://www.google.com/schemas/sitemap-video/1.1"> # noqa: E501
        <url>
            <loc>https://www.foo.come/terms-service</loc>
            <changefreq>daily</changefreq>
            <priority>0.7</priority>
        </url>
        <url>
            <loc>https://www.foo.come/policy</loc>
            <changefreq>daily</changefreq>
            <priority>0.7</priority>
        </url>
        <url>
            <loc>https://www.foo.come/policy/security</loc>
            <changefreq>daily</changefreq>
            <priority>0.7</priority>
        </url>
    </urlset>
    """
    expected_output = [
        {'scraped_at': SCRAPED_AT, 'url': 'https://www.foo.come/terms-service'},
        {'scraped_at': SCRAPED_AT, 'url': 'https://www.foo.come/policy'},
        {'scraped_at': SCRAPED_AT, 'url': 'https://www.foo.come/policy/security'},
    ]

    task = fetch_sitemap
    with context(date=SCRAPED_AT):
        sitemap_filename, sitemap_json = task.run(sitemap_url='https://www.foo.com/sitemap-0.xml')
    assert (sitemap_filename, json.loads(sitemap_json)) == ('sitemap-0', expected_output)
