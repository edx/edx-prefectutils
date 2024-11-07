"""
Tests for sitemap functions.
pytest test_sitemap.py
"""

import json
import unittest
from unittest.mock import Mock, patch
import requests
from edx_argoutils.sitemap import fetch_sitemap, fetch_sitemap_urls

SCRAPED_AT = '2021-10-22T15:14:16.683985+00:00'


class TestSitemapTasks(unittest.TestCase):

    @patch.object(requests, 'get')
    def test_fetch_sitemap_urls(self, mockget):
        # Mock the response from requests.get
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

        # Expected output for the mock sitemap index response
        expected_output = ['https://www.foo.com/sitemap-0.xml', 'https://www.foo.com/sitemap-1.xml']

        # Call the function (directly, without Prefect context)
        result = fetch_sitemap_urls(sitemap_index_url='dummy_url')

        # Check if the result matches the expected output
        self.assertEqual(result, expected_output)


    @patch.object(requests, 'get')
    def test_fetch_sitemap(self, mockget):
        # Mock the response from requests.get
        mockresponse = Mock()
        mockget.return_value = mockresponse
        mockresponse.text = """<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:video="http://www.google.com/schemas/sitemap-video/1.1">
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

        # Expected output for the mock sitemap response
        expected_output = [
            {'scraped_at': SCRAPED_AT, 'url': 'https://www.foo.come/terms-service'},
            {'scraped_at': SCRAPED_AT, 'url': 'https://www.foo.come/policy'},
            {'scraped_at': SCRAPED_AT, 'url': 'https://www.foo.come/policy/security'},
        ]

        # Manually pass SCRAPED_AT (instead of using Prefect context)
        sitemap_url = 'https://www.foo.com/sitemap-0.xml'
        sitemap_filename, sitemap_json = fetch_sitemap(sitemap_url=sitemap_url, scraped_at=SCRAPED_AT)

        # Check if the result matches the expected output
        self.assertEqual((sitemap_filename, json.loads(sitemap_json)), ('sitemap-0', expected_output))


if __name__ == '__main__':
    unittest.main()
