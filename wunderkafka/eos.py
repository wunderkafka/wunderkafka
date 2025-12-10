from types import TracebackType
from typing import Optional

from wunderkafka.logger import logger
from wunderkafka.consumers.abc import AbstractConsumer, AbstractDeserializingConsumer
from wunderkafka.producers.abc import AbstractProducer, AbstractSerializingProducer


class EOSTransaction:
    def __init__(
        self,
        producer: AbstractProducer | AbstractSerializingProducer,
        consumer: Optional[AbstractConsumer | AbstractDeserializingConsumer] = None,
    ) -> None:
        self.producer = producer
        self.consumer = consumer

    def __enter__(self) -> "EOSTransaction":
        if not self.producer.transaction_ready:
            logger.debug("Initializing transactions for the producer.")
            self.producer.prepare_transactions()
        self.producer.begin_transaction()
        self.producer.poll(0)
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

    def __exit__(
        self, exc_type: Optional[type[Exception]], exc_value: Optional[Exception], exc_tb: Optional[TracebackType]
    ) -> Optional[bool]:
        if exc_type is None:
            self._commit()
            return True
        self._abort()
        return False
