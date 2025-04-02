from dataclasses import dataclass
from typing import Any, Optional, Union
from unittest.mock import Mock

from wunderkafka.callbacks import error_callback
from wunderkafka.producers.bytes import BytesProducer
from wunderkafka.types import DeliveryCallback


@dataclass
class Message:
    topic: str
    value: Optional[Union[str, bytes]]
    key: Optional[Union[str, bytes]]


class TestProducer(BytesProducer):
    init_transactions: Mock
    begin_transaction: Mock
    commit_transaction: Mock
    abort_transaction: Mock
    send_offsets_to_transaction: Mock

    def __init__(self) -> None:
        self.sent: list[Message] = []
        self.init_transactions = Mock()
        self.begin_transaction = Mock()
        self.commit_transaction = Mock()
        self.abort_transaction = Mock()
        self.send_offsets_to_transaction = Mock()


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
        message = Message(topic, value, key)
        self.sent.append(message)
