from typing import Optional

from wunderkafka.consumers.abc import AbstractConsumer
from wunderkafka.producers.abc import AbstractProducer


class EOSTransaction:

    def __init__(
        self, 
        producer: AbstractProducer,
        consumer: Optional[AbstractConsumer] = None,
    ) -> None:
        self.producer = producer
        self.consumer = consumer

    def __enter__(self) -> None:
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

    def __exit__(self, exc_type: type, exc_value: Exception, exc_tb) -> Optional[bool]:
        if exc_type is None:
            self._commit()
            return
        self._abort()
        return False

