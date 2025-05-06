from __future__ import annotations

import datetime
from typing import Any

from confluent_kafka import KafkaError

from wunderkafka.consumers.bytes import BytesConsumer
from wunderkafka.consumers.subscription import TopicSubscription


class Message:
    def __init__(self, topic: str, value: bytes, key: bytes | None = None, error: KafkaError | None = None):
        self._topic = topic
        self._value = value
        self._key = key
        self._error = error

    def value(self) -> bytes:
        return self._value

    def set_value(self, value: Any) -> None:
        self._value = value

    def key(self) -> bytes | None:
        return self._key

    def set_key(self, key: Any) -> None:
        self._key = key

    def topic(self) -> str:
        return self._topic

    def error(self) -> KafkaError | None:
        return self._error


class TestConsumer(BytesConsumer):
    def __init__(self, msgs: list[Message]) -> None:
        self._msgs = msgs

    def subscribe(
        self,
        topics: list[str | TopicSubscription],
        *,
        from_beginning: bool | None = None,
        offset: int | None = None,
        ts: float | None = None,
        dt: datetime.datetime | None = None,
        with_timedelta: datetime.timedelta | None = None,
    ) -> None:
        pass

    def batch_poll(
        self,
        timeout: float = 1.0,
        num_messages: int = 1000000,
        *,
        raise_on_lost: bool = False,
    ) -> list[Message]:
        return self._msgs
