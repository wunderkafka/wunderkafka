import os
from typing import Any

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

from wunderkafka.time import ts2dt
from wunderkafka.logger import logger


# as the librdkafka on `sasl.oauthbearer.config` doc says,
#   "The format is implementation-dependent and must be parsed accordingly"
# so we can't annotate `_` with something meaningful.
# we just don't use it for now
def oauth_callback(_: Any) -> tuple[str, int]:  # noqa: ANN401
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    if not region:
        msg = "Region not set - export AWS_REGION or AWS_DEFAULT_REGION before running."
        raise RuntimeError(msg)
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
    # Note that this library expects oauth_cb to return expiry time in seconds since epoch,
    # while the token generator returns expiry in ms
    logger.info(f"Generated auth token for region {region} with expiry {ts2dt(expiry_ms)}")
    return auth_token, expiry_ms / 1000
