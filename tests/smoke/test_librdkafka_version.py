from __future__ import annotations

import pytest
from confluent_kafka.cimpl import libversion

from wunderkafka.librdkafka import get_librdkafka_version_tuple


@pytest.mark.parametrize(
    ("version", "answer"),
    [
        ("1.0.0", (1, 0, 0)),
        ("1.0.0-beta.1", (1, 0, 0)),
        ("1.0.0-beta.1.1", (1, 0, 0)),
        (("2.11.1", 34275839), (2, 11, 1)),
        (("1.6.0", 17170687), (1, 6, 0)),
        (("1.6.0-whatever", 17170687), (1, 6, 0)),
        ("2.12.0", (2, 12, 0)),
    ],
)
def test_get_librdkafka_version_tuple(version: str | tuple[str, int], answer: tuple[int, ...]) -> None:
    assert get_librdkafka_version_tuple(version) == answer


def test_get_librdkafka_version_smoke() -> None:
    assert get_librdkafka_version_tuple(libversion())
