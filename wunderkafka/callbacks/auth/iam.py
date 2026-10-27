import os
import threading
from typing import Any
from unittest.mock import patch

import botocore.session
from botocore.exceptions import ClientError, BotoCoreError
from botocore.credentials import Credentials, CredentialProvider, RefreshableCredentials
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

from wunderkafka.time import ts2dt
from wunderkafka.logger import logger

Token = str
ExpiryMs = int


class _SnapshotCredentialProvider(CredentialProvider):
    # one frozen snapshot, so a refresh between reads of key, secret and token can't mix two sets
    METHOD = "wunderkafka-snapshot"

    def __init__(self, credentials: Credentials) -> None:
        super().__init__()
        self._credentials = credentials

    def load(self) -> Credentials:
        frozen = self._credentials.get_frozen_credentials()
        return Credentials(frozen.access_key, frozen.secret_key, frozen.token)


class OAuthCallback:
    """MSK IAM ``oauth_cb``: refreshable credentials are resolved once, static ones on every call."""

    def __init__(self, session_name: str):
        self.session_name = session_name
        self._cached: RefreshableCredentials | None = None
        self._lock = threading.Lock()

    def _resolve(self) -> Credentials:
        with patch.dict(os.environ, {"AWS_ROLE_SESSION_NAME": self.session_name}):
            credentials = botocore.session.Session().get_credentials()

        if credentials is None:
            msg = "No AWS credentials found for MSK IAM auth"
            raise RuntimeError(msg)
        return credentials

    def _credentials(self) -> Credentials:
        if self._cached is not None:
            return self._cached

        credentials = self._resolve()
        if isinstance(credentials, RefreshableCredentials):
            self._cached = credentials
        return credentials

    def _token(self, region: str) -> tuple[Token, ExpiryMs]:
        credentials = self._credentials()

        provider = _SnapshotCredentialProvider(credentials)
        auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token_from_credentials_provider(region, provider)
        logger.info(f"Generated auth token for region {region} with expiry {ts2dt(expiry_ms)}")
        return auth_token, expiry_ms / 1000

    def __call__(self, _: Any) -> tuple[Token, ExpiryMs]:  # noqa: ANN401
        region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
        if not region:
            msg = "Region not set - export AWS_REGION or AWS_DEFAULT_REGION before running."
            raise RuntimeError(msg)

        with self._lock:
            try:
                return self._token(region)
            except (BotoCoreError, ClientError):
                if self._cached is None:
                    raise
                self._cached = None
                logger.warning("Cached AWS credentials failed to renew, resolving them again", exc_info=True)
                return self._token(region)


# as the librdkafka on `sasl.oauthbearer.config` doc says,
#   "The format is implementation-dependent and must be parsed accordingly"
# so we can't annotate `_` with something meaningful.
# we just don't use it for now
def oauth_callback(_: Any) -> tuple[Token, ExpiryMs]:  # noqa: ANN401
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    if not region:
        msg = "Region not set - export AWS_REGION or AWS_DEFAULT_REGION before running."
        raise RuntimeError(msg)
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
    # Note that this library expects oauth_cb to return expiry time in seconds since epoch,
    # while the token generator returns expiry in ms
    logger.info(f"Generated auth token for region {region} with expiry {ts2dt(expiry_ms)}")
    return auth_token, expiry_ms / 1000
