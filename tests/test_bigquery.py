#!/usr/bin/env python

"""
Tests for BigQuery utils in the `edx_prefectutils` package.
"""

from prefect.core import Flow
from pytest_mock import mocker  # noqa: F401

from edx_prefectutils import bigquery


def test_cleanup_gcs_files(mocker):  # noqa: F811
    mocker.patch.object(bigquery, 'get_storage_client')
    with Flow("test") as f:
        bigquery.cleanup_gcs_files(
            gcp_credentials={},
            url='',
            project='test_project'
        )
    state = f.run()
    assert state.is_successful()


def test_extract_ga_table(mocker):  # noqa: F811
    mocker.patch.object(bigquery, 'get_bigquery_client')
    with Flow("test") as f:
        task = bigquery.extract_ga_table(
            project='test_project',
            gcp_credentials={},
            dataset='test_dataset',
            date='2020-01-01',
            output_root='test_output_root'
        )
    state = f.run()
    assert state.is_successful()
    assert state.result[task].result == "test_output_root/test_dataset/2020-01-01"
