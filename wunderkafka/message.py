from __future__ import annotations

from typing import Protocol, runtime_checkable


# `@runtime_checkable` is needed for pydantic to create schema in StreamResult
@runtime_checkable
class KafkaErrorProtocol(Protocol):
    def code(self) -> int: ...


# `@runtime_checkable` is needed for pydantic to create schema in StreamResult
@runtime_checkable
class MessageProtocol(Protocol):
    """
    Protocol for message-like objects.

    This is used to ensure that the `MessageStub` class can be used in places where a message-like object is expected.
    """

    def error(self) -> None | KafkaErrorProtocol: ...

    def headers(self) -> list[tuple[str, bytes]] | None: ...

    def key(self) -> object: ...

    def offset(self) -> int | None: ...

    def partition(self) -> int | None: ...

    def set_headers(self, headers: list[tuple[str, bytes]]) -> None: ...

    def set_key(self, key: object) -> None: ...

    def set_value(self, value: object) -> None: ...

    def timestamp(self) -> tuple[int, int]: ...

    def topic(self) -> str | None: ...

    def value(self) -> object: ...
