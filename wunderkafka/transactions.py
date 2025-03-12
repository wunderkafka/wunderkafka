from types import TracebackType
from typing import Optional, Union

from wunderkafka.consumers.abc import AbstractConsumer
from wunderkafka.consumers.constructor import HighLevelDeserializingConsumer
from wunderkafka.producers.abc import AbstractProducer
from wunderkafka.producers.constructor import HighLevelSerializingProducer


class EOSTransaction:

    def __init__(
        self, 
        producer: Union[AbstractProducer, HighLevelSerializingProducer],
        consumer: Optional[Union[AbstractConsumer, HighLevelDeserializingConsumer]] = None,
    ) -> None:
        self.producer = producer
        self.consumer = consumer

    def __enter__(self) -> "EOSTransaction":
        self.producer.begin_transaction()
        return self

    def _abort(self) -> None:
        self.producer.abort_transaction()

    def _commit(self) -> None:
        if self.consumer is not None:
            self.producer.send_offsets_to_transaction(
                self.consumer.position(self.consumer.assignment()),
                self.consumer.consumer_group_metadata(),
            )
        self.producer.commit_transaction()

    def __exit__(self, exc_type: Optional[type[Exception]], exc_value: Optional[Exception], exc_tb: Optional[TracebackType]) -> Optional[bool]:
        if exc_type is None:
            self._commit()
            return True
        self._abort()
        return False

