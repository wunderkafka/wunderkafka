from types import TracebackType
from typing import Optional

from wunderkafka.consumers.abc import AbstractConsumer
from wunderkafka.producers.abc import AbstractProducer


class EOSTransaction:

    def __init__(
        self, 
        producer: AbstractProducer,
        consumer: Optional[AbstractConsumer] = None,
    ) -> None:
        assert producer.transaction_ready, "You must call producer.prepare_transactions() before using EOSTransaction."
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

