from __future__ import annotations

import datetime

from wunderkafka.message import MessageProtocol as Message
from wunderkafka.consumers.bytes import BytesConsumer
from wunderkafka.consumers.subscription import TopicSubscription


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
