from typing import Any, Union, Optional

from wunderkafka.types import DeliveryCallback
from wunderkafka.message import MessageProtocol as Message
from wunderkafka.tests.message import Message as MessageStub
from wunderkafka.producers.bytes import BytesProducer
from wunderkafka.callbacks.producer import error_callback


class TestProducer(BytesProducer):
    def __init__(self) -> None:
        self.sent: list[Message] = []

    def send_message(
        self,
        topic: str,
        value: Optional[Union[str, bytes]] = None,
        key: Optional[Union[str, bytes]] = None,
        partition: Optional[int] = None,
        on_delivery: Optional[DeliveryCallback] = error_callback,
        *args: Any,
        blocking: bool = False,
        **kwargs: Any,
    ) -> None:
        message = MessageStub(topic=topic, value=value, key=key)
        self.sent.append(message)
