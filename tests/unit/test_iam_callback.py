from __future__ import annotations

import os
import base64
import datetime
from urllib.parse import parse_qs, urlparse

import pytest

pytest.importorskip("botocore")
pytest.importorskip("aws_msk_iam_sasl_signer")

import botocore.session  # noqa: E402
from botocore.exceptions import ClientError, RefreshWithMFAUnsupportedError  # noqa: E402
from botocore.credentials import RefreshableCredentials  # noqa: E402

from wunderkafka.callbacks.auth.iam import OAuthCallback  # noqa: E402

SESSION_NAME = "botocore-session-test"


@pytest.fixture(autouse=True)
def static_aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIDEXAMPLE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("AWS_REGION", "ap-southeast-1")


@pytest.fixture
def sessions_created(monkeypatch: pytest.MonkeyPatch) -> list[botocore.session.Session]:
    created: list[botocore.session.Session] = []
    original_init = botocore.session.Session.__init__

    def tracking_init(self: botocore.session.Session, *args: object, **kwargs: object) -> None:
        created.append(self)
        original_init(self, *args, **kwargs)

    monkeypatch.setattr(botocore.session.Session, "__init__", tracking_init)
    return created


def _rotating_credentials(expires_in: datetime.timedelta) -> RefreshableCredentials:
    issued = iter(range(1, 100))

    def refresh() -> dict[str, str]:
        n = next(issued)
        return {
            "access_key": f"ASIAKEY{n}",
            "secret_key": f"secret{n}",
            "token": f"token{n}",
            "expiry_time": (datetime.datetime.now(tz=datetime.timezone.utc) + expires_in).isoformat(),
        }

    return RefreshableCredentials.create_from_metadata(refresh(), refresh, "test")


def _failing_to_renew(error: Exception) -> RefreshableCredentials:
    def refresh() -> dict[str, str]:
        raise error

    metadata = {
        "access_key": "ASIASTALE",
        "secret_key": "stale",
        "token": "stale",
        "expiry_time": (datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(minutes=5)).isoformat(),
    }
    return RefreshableCredentials.create_from_metadata(metadata, refresh, "test")


RENEWAL_FAILURES = [
    pytest.param(RefreshWithMFAUnsupportedError(), id="mfa-profile"),
    pytest.param(
        ClientError({"Error": {"Code": "InvalidClientTokenId"}}, "AssumeRole"),
        id="rotated-source-profile-keys",
    ),
]


def _signed_query(token: str) -> dict[str, list[str]]:
    url = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)).decode()
    return parse_qs(urlparse(url).query)


def _access_key(token: str) -> str:
    return _signed_query(token)["X-Amz-Credential"][0].split("/")[0]


def test_credentials_are_resolved_once(
    monkeypatch: pytest.MonkeyPatch,
    sessions_created: list[botocore.session.Session],
) -> None:
    credentials = _rotating_credentials(expires_in=datetime.timedelta(hours=1))
    monkeypatch.setattr(botocore.session.Session, "get_credentials", lambda _: credentials)
    callback = OAuthCallback(SESSION_NAME)

    first_token, first_expiry = callback(None)
    second_token, _ = callback(None)

    assert len(sessions_created) == 1
    assert _access_key(first_token) == _access_key(second_token) == "ASIAKEY1"
    url = base64.urlsafe_b64decode(first_token + "==").decode()
    assert url.startswith("https://kafka.ap-southeast-1.amazonaws.com/")
    assert first_expiry > datetime.datetime.now(tz=datetime.timezone.utc).timestamp()


def test_token_is_signed_with_one_consistent_refreshed_credentials_set(monkeypatch: pytest.MonkeyPatch) -> None:
    # inside botocore's mandatory refresh window every read of live credentials refreshes them
    credentials = _rotating_credentials(expires_in=datetime.timedelta(minutes=5))
    monkeypatch.setattr(botocore.session.Session, "get_credentials", lambda _: credentials)
    callback = OAuthCallback(SESSION_NAME)

    first = _signed_query(callback(None)[0])
    second = _signed_query(callback(None)[0])

    first_key = first["X-Amz-Credential"][0].split("/")[0]
    second_key = second["X-Amz-Credential"][0].split("/")[0]
    assert first_key != second_key
    assert first["X-Amz-Security-Token"] == [first_key.replace("ASIAKEY", "token")]
    assert second["X-Amz-Security-Token"] == [second_key.replace("ASIAKEY", "token")]


@pytest.mark.parametrize("error", RENEWAL_FAILURES)
def test_credentials_that_fail_to_renew_are_resolved_again_on_the_next_call(
    monkeypatch: pytest.MonkeyPatch,
    sessions_created: list[botocore.session.Session],
    error: Exception,
) -> None:
    resolutions = iter([_failing_to_renew(error), _rotating_credentials(expires_in=datetime.timedelta(hours=1))])
    monkeypatch.setattr(botocore.session.Session, "get_credentials", lambda _: next(resolutions))
    callback = OAuthCallback(SESSION_NAME)

    with pytest.raises(type(error)):
        callback(None)
    token, _ = callback(None)

    assert len(sessions_created) == 2
    assert _access_key(token) == "ASIAKEY1"


@pytest.mark.parametrize(
    ("session_name", "expected"),
    [
        pytest.param(SESSION_NAME, SESSION_NAME, id="named-overrides-env"),
        pytest.param(None, "from-env", id="unnamed-keeps-env"),
    ],
)
def test_role_session_name_is_set_only_when_given(
    monkeypatch: pytest.MonkeyPatch,
    session_name: str | None,
    expected: str,
) -> None:
    monkeypatch.setenv("AWS_ROLE_SESSION_NAME", "from-env")
    seen: list[str | None] = []

    def get_credentials(_: botocore.session.Session) -> RefreshableCredentials:
        seen.append(os.environ.get("AWS_ROLE_SESSION_NAME"))
        return _rotating_credentials(expires_in=datetime.timedelta(hours=1))

    monkeypatch.setattr(botocore.session.Session, "get_credentials", get_credentials)

    OAuthCallback(session_name)(None)

    assert seen == [expected]
