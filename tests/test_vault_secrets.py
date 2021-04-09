#!/usr/bin/env python

"""
Tests for Hashicorp Vault secrets utils in the `edx_prefectutils` package.
"""

from prefect import Flow, task, unmapped
from pytest_mock import mocker  # noqa: F401

from edx_prefectutils import vault_secrets


@task
def get_val(secret):
    return secret.get("test")


def test_read_vault_secret(mocker):  # noqa: F811
    mocker.patch.object(vault_secrets, 'open')
    mocker.patch.object(vault_secrets.hvac, 'Client')
    with Flow("test") as f:
        secret_val = vault_secrets.VaultKVSecret(
            path="warehouses/test_platform/test_secret",
            version=2
        )
        get_val(unmapped(secret_val))
    state = f.run()
    assert state.is_successful()
