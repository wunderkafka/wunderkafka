import os
from typing import Any
from unittest.mock import patch

import botocore.session
from botocore.exceptions import ClientError, BotoCoreError
from botocore.credentials import Credentials, CredentialProvider, ReadOnlyCredentials
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

from wunderkafka.time import ts2dt
from wunderkafka.logger import logger

Token = str
ExpiryMs = int


class _FrozenCredentialProvider(CredentialProvider):
    # the signer reads key, secret and token one by one; a refresh in between would mix two sets
    METHOD = "wunderkafka-frozen"

    def __init__(self, credentials: Credentials) -> None:
        super().__init__()
        self._credentials = credentials

    def load(self) -> ReadOnlyCredentials:
        return self._credentials.get_frozen_credentials()


class OAuthCallback:
    """MSK IAM ``oauth_cb`` that resolves AWS credentials once: botocore renews them itself."""

    def __init__(self, session_name: str | None = None):
        self.session_name = session_name
        self._credentials: Credentials | None = None

    def _resolve(self) -> Credentials:
        # `generate_auth_token` builds a new botocore session on every call, so resolve once and reuse
        environ = {"AWS_ROLE_SESSION_NAME": self.session_name} if self.session_name else {}
        with patch.dict(os.environ, environ):
            credentials = botocore.session.Session().get_credentials()
        if credentials is None:
            msg = "No AWS credentials found for MSK IAM auth"
            raise RuntimeError(msg)
        return credentials

    # as the librdkafka on `sasl.oauthbearer.config` doc says,
    #   "The format is implementation-dependent and must be parsed accordingly"
    # so we can't annotate `_` with something meaningful.
    # we just don't use it for now
    def __call__(self, _: Any) -> tuple[Token, ExpiryMs]:  # noqa: ANN401
        region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
        if not region:
            msg = "Region not set - export AWS_REGION or AWS_DEFAULT_REGION before running."
            raise RuntimeError(msg)

        if self._credentials is None:
            self._credentials = self._resolve()
        provider = _FrozenCredentialProvider(self._credentials)
        try:
            auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token_from_credentials_provider(region, provider)
        except (BotoCoreError, ClientError):
            # failed renewal leaves the cached credentials dead; librdkafka retries and resolves them anew
            self._credentials = None
            raise
        logger.info(f"Generated auth token for region {region} with expiry {ts2dt(expiry_ms)}")
        # Note that this library expects oauth_cb to return expiry time in seconds since epoch,
        # while the token generator returns expiry in ms
        return auth_token, expiry_ms / 1000
