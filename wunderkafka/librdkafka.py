from __future__ import annotations

from confluent_kafka import libversion


def get_librdkafka_version_tuple(version: str | tuple[str, int]) -> tuple[int, ...]:
    if not isinstance(version, str):
        _triplet, _ = version
    else:
        _triplet = version
    if "-" in _triplet:
        _triplet = _triplet.split("-")[0]
    return tuple(int(digit) for digit in _triplet.split("."))


__version__ = get_librdkafka_version_tuple(libversion())
