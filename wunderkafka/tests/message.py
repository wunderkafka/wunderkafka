from __future__ import annotations

import time

from wunderkafka.compat import Self
from wunderkafka.message import MessageProtocol, KafkaErrorProtocol


class KafkaError(KafkaErrorProtocol):
    def __init__(self, code: int = 25) -> None:
        self._code = code

    def code(self) -> int:
        return self._code

    def __str__(self) -> str:
        # KafkaError{code=UNKNOWN_MEMBER_ID,val=25,str="Commit failed: Broker: Unknown member"}
        return f'KafkaError{{code={self.code()},val={self.code()},str="{self.code()}"}}'


class Message(MessageProtocol):
    """
    Represents a message-like

    For a full reference, see the Confluent Kafka Python documentation:
        https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#message

    """

    def __init__(
        self,
        value: object,
        key: object | None = None,
        headers: list[tuple[str, bytes]] | None = None,
        topic: str | None = None,
        partition: int | None = None,
        offset: int | None = None,
        timestamp: tuple[int, int] | None = None,
        error: KafkaError | None = None,
    ) -> None:
        self._topic = topic
        self._value = value
        self._key = key
        self._error = error
        self._headers = headers
        self._partition = partition
        self._offset = offset
        self._timestamp = timestamp

    def error(self) -> None | KafkaError:
        return self._error

    def headers(self) -> list[tuple[str, bytes]] | None:
        return self._headers

    def key(self) -> object:
        return self._key

    def offset(self) -> int | None:
        return self._offset

    def partition(self) -> int | None:
        return self._partition

    # Strictly speaking, it also may be Any.
    # But as set_value and set_key are really used to override deserialized message,
    # and the headers are not - let's try to keep it in a more strict way.
    def set_headers(self, headers: list[tuple[str, bytes]]) -> None:
        self._headers = headers

    def set_key(self, key: object) -> None:
        self._key = key

    def set_value(self, value: object) -> None:
        self._value = value

    def timestamp(self) -> tuple[int, int]:
        if self._timestamp is None:
            # The returned timestamp should be ignored if the timestamp type is TIMESTAMP_NOT_AVAILABLE.
            return (0, -1)
        return self._timestamp

    def topic(self) -> str | None:
        return self._topic

    def value(self) -> object:
        return self._value

    @classmethod
    def dummy(cls, topic: str, value: object, key: object | None = None) -> Self:
        return cls(
            value=value,
            key=key,
            topic=topic,
            partition=1,
            offset=1,
            timestamp=(1, int(time.time() * 1000)),
        )
