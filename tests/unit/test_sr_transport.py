import json
import base64
import logging
import threading
from typing import Any
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from collections.abc import Iterator

import pytest

from wunderkafka.config.schema_registry import SRConfig
from wunderkafka.schema_registry.transport import KerberizableHTTPClient

PASSWORD = "S3cretPassw0rd"
AUTH_HEADER = "Basic " + base64.b64encode(f"ml:{PASSWORD}".encode()).decode()


class StubRegistry:
    def __init__(self) -> None:
        self.status = 200
        self.authorizations: list[str | None] = []
        self.paths: list[str] = []
        self.port = 0


class RegistryServer(ThreadingHTTPServer):
    def __init__(self, registry: StubRegistry) -> None:
        super().__init__(("127.0.0.1", 0), StubHandler)
        self.registry = registry


class StubHandler(BaseHTTPRequestHandler):
    server: RegistryServer

    def handle_request(self) -> None:
        registry = self.server.registry
        registry.authorizations.append(self.headers.get("Authorization"))
        registry.paths.append(self.path)
        body = json.dumps({"id": 1}).encode()
        self.send_response(registry.status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_POST = handle_request  # noqa: N815  # http.server dispatches on these exact names

    def log_message(self, *args: Any) -> None:
        return None


@pytest.fixture
def registry() -> Iterator[StubRegistry]:
    stub = StubRegistry()
    server = RegistryServer(stub)
    stub.port = server.server_port
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield stub
    server.shutdown()
    server.server_close()
    thread.join()


def test_password_is_not_leaked_when_registry_is_unavailable(
    registry: StubRegistry,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    registry.status = 503
    client = KerberizableHTTPClient(SRConfig(url=f"http://ml:{PASSWORD}@127.0.0.1:{registry.port}"))

    with pytest.raises(Exception) as exc_info:  # noqa: PT011, B017  # error type depends on the installed HTTP backend
        client.make_request("subjects/topic-value/versions?normalize=False", "POST", body={"schema": "x"})

    assert PASSWORD not in str(exc_info.value)
    assert PASSWORD not in caplog.text
    assert f"http://127.0.0.1:{registry.port}/subjects/topic-value/versions" in str(exc_info.value)


def test_credentials_from_url_are_sent_as_basic_auth(registry: StubRegistry) -> None:
    client = KerberizableHTTPClient(SRConfig(url=f"http://ml:{PASSWORD}@127.0.0.1:{registry.port}"))

    client.make_request("subjects", "GET")

    assert registry.authorizations == [AUTH_HEADER]
    assert registry.paths == ["/subjects"]


def test_percent_encoded_password_is_decoded(registry: StubRegistry) -> None:
    client = KerberizableHTTPClient(SRConfig(url=f"http://ml:p%40ss%3Aword@127.0.0.1:{registry.port}"))

    client.make_request("subjects", "GET")

    assert registry.authorizations == ["Basic " + base64.b64encode(b"ml:p@ss:word").decode()]


def test_url_without_credentials_is_unchanged(registry: StubRegistry) -> None:
    url = f"http://127.0.0.1:{registry.port}/api/v1"
    client = KerberizableHTTPClient(SRConfig(url=url))

    client.make_request("subjects", "GET")

    assert client.base_url == url
    assert registry.authorizations == [None]
    assert registry.paths == ["/api/v1/subjects"]


def test_base_url_has_no_credentials_but_keeps_port_and_path() -> None:
    config = SRConfig(url=f"https://ml:{PASSWORD}@schema-registry.example:8081/api")

    client = KerberizableHTTPClient(config)

    assert client.base_url == "https://schema-registry.example:8081/api"
    client.close()
