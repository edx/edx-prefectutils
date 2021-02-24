"""
Tasks for fetching secrets from Vault.  There is a separate class per Vault
"engine", and they should all extend VaultSecretBase which centralizes the
logic to construct a Vault client from a Service Account JWT token.

Example usage for VaultKVSecret:

1. Create a KV secret at <https://vault.analytics.edx.org/ui/vault/secrets/kv/list>.

2. If, for example, the new secret had a path of
"snowflake_pipeline_etl_loader", use the following code pattern to use the
secret in Prefect flows:

    from vault_secrets import VaultKVSecret

    @task
    def load_data_into_snowflake(sf_credentials):
        self.logger.info("logging into Snowflake with username: {}".format(credentials["user"]))
        connection = create_snowflake_connection(credentials, role)
        ...
        connection.close()

    with Flow("Load Data Into Snowflake") as flow:
        sf_credentials = VaultKVSecret(
            path="snowflake_pipeline_etl_loader",
            version=3,
        )
        load_data_into_snowflake(sf_credentials)
"""
import hvac
from prefect.tasks.secrets import SecretBase

# This is a standardized k8s path to always find the service account JWT token.
SERVICE_ACCOUNT_JWT_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"

# Global configuration for accessing vault from all Prefect flows inside of the analytics k8s
# cluster.
VAULT_BASE_URL = "https://vault.analytics.edx.org"
VAULT_LOGIN_URL = VAULT_BASE_URL + "/v1/auth/kubernetes/login"
VAULT_ROLE = "prefect"

# Global configuration for accessing vault from Prefect flows _outside_ of K8s.
EXTERNAL_VAULT_BASE_URL = "https://vault.analytics.edx.org"


class VaultSecretBase(SecretBase):
    """
    A base Secret task that establishes the vault client which can be used by
    extending classes to fetch secrets from Vault.

    Extending classes should override self._get_secret(), and use
    self.vault_client (instance of hvac.Client) to make requests.

    Args:
        **kwargs (Any, optional): additional keyword arguments to pass to the Task constructor
    """

    @staticmethod
    def _get_k8s_vault_client() -> hvac.Client:
        """
        Convert the current container's service account JWT token into a Vault
        token and use that to construct a Vault client.
        """
        with open(SERVICE_ACCOUNT_JWT_TOKEN_PATH) as sa_token_file:
            service_account_token = sa_token_file.read()
        client = hvac.Client(url=VAULT_BASE_URL)
        client.auth_kubernetes(role=VAULT_ROLE, jwt=service_account_token)
        return client

    @staticmethod
    def _get_env_var_vault_client() -> hvac.Client:
        """
        For local development, if the user is logged into Vault we can use
        their existing environment vars.
        """
        client = hvac.Client(url=EXTERNAL_VAULT_BASE_URL)
        return client

    def run(self):
        # First try k8s auth, if we can't find the magic file then try local env var authentication.
        try:
            self.vault_client = self._get_k8s_vault_client()
        except FileNotFoundError:
            # If that fails try local token auth
            self.vault_client = self._get_env_var_vault_client()

        if not self.vault_client.is_authenticated():
            raise Exception("Vault Client error. We don't seem to be in K8s and no Vault token found. "
                            "Try 'vault login -address https://vault.analytics.edx.org -method oidc' "
                            "if you've downloaded Vault.")

        return self._get_secret()

    def _get_secret(self):
        """
        Override this in extending classes to fetch the secret using
        `self.vault_client`.
        """
        pass


class VaultKVSecret(VaultSecretBase):
    """
    A `Secret` prefect task for fetching KV secrets from Vault. Note that this
    only supports version 2 of the KV engine.

    Manage KV secrets at https://vault.analytics.edx.org/ui/vault/secrets/kv/list

    Args:
        path (str): The path of the KV secret, e.g. "snowflake_pipeline_etl_loader".
        version (int): The version number of the KV secret.
        **kwargs (Any, optional): Additional keyword arguments to pass to the Task constructor.
    """

    def __init__(self, path: str, version: int, **kwargs):
        self.kv_path = path
        self.kv_version = version
        super().__init__(**kwargs)

    def _get_secret(self):
        """
        Fetch the KV secret specified by path and version.

        Returns:
            dict containing the key/value pairs, where the values are secrets.
        """
        secret_version_response = self.vault_client.secrets.kv.v2.read_secret_version(
            mount_point="kv", path=self.kv_path, version=self.kv_version,
        )
        return secret_version_response["data"]["data"]
