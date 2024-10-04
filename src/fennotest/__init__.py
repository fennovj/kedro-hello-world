from kedro.io import AbstractDataset
import snowflake.connector
import pandas as pd
from typing import Any, Dict
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

def get_snowflake_key(private_key: str, private_key_passphrase: str) -> bytes:
    """Given a private key and passphrase, decrypts it into bytes, with 'DER' encoding.
    This bytes object can be used by the `snowflake.connector` and `sqlalchemy` packages to authenticate to snowflake.

    :param str private_key: Private key. Should look like: -----BEGIN ENCRYPTED PRIVATE KEY----- ... -----END ENCRYPTED PRIVATE KEY-----
    :param str private_key_passphrase: Passphrase used to encrypt the private key
    :return bytes: Bytes object containing the decrypted private key in DER encoding.
    """
    assert (
        serialization and default_backend
    ), "You don't have snowflake dependencies installed. Install with `pip install databaas[snowflake]`"
    assert private_key and private_key_passphrase
    p_key = serialization.load_pem_private_key(
        private_key.encode(),
        password=private_key_passphrase.encode(),
        backend=default_backend(),
    )
    private_key_bytes = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return private_key_bytes

class SnowflakeQueryDataset(AbstractDataset[pd.DataFrame, pd.DataFrame]):
    def __init__(self, query: str, account: str, credentials: dict):
        self._query = query
        with open(credentials['private_key_path'], 'r') as f:
            private_key = f.read()
        credentials['private_key'] = get_snowflake_key(private_key, credentials['private_key_passphrase'])
        credentials['account'] = account
        self._credentials = credentials

    def _load(self) -> pd.DataFrame:
        with snowflake.connector.connect(**self._credentials) as conn:
            with conn.cursor() as cur:
                cur.execute(self._query)
                return pd.DataFrame(cur.fetchall(), columns=[col[0] for col in cur.description])

    def _save(self, data: pd.DataFrame) -> None:
        raise NotImplementedError("Save is not implemented for SnowflakeQueryDataset")

    def _exists(self) -> bool:
        return False

    def _describe(self) -> Dict[str, Any]:
        return dict()

    def _release(self) -> None:
        pass