"""This module contains implementation of extended confluent-kafka Producer's API."""

from __future__ import annotations

import atexit

from confluent_kafka import KafkaException

from wunderkafka.types import DeliveryCallback
from wunderkafka.compat import ParamSpec
from wunderkafka.config import ProducerConfig
from wunderkafka.callbacks import error_callback
from wunderkafka.producers.abc import AbstractProducer
from wunderkafka.config.krb.rdkafka import challenge_krb_arg

P = ParamSpec("P")


class BytesProducer(AbstractProducer):
    """Producer implementation of extended interface for raw messages."""

    def __init__(self, config: ProducerConfig) -> None:
        """
        Init producer.

        :param config:          Pydantic model with librdkafka producer's configuration.
        """
        try:
            super().__init__(config.dict())
        except KafkaException as exc:
            config = challenge_krb_arg(exc, config)
            super().__init__(config.dict())

        self._config = config
        # TODO (tribunsky.kir): make it configurable
        #                       https://github.com/severstal-digital/wunderkafka/issues/90
        atexit.register(self.flush)

    # TODO (tribunsky.kir): Do we need re-initiation of consumer/producer in runtime?
    #                       https://github.com/severstal-digital/wunderkafka/issues/94
    @property
    def config(self) -> ProducerConfig:
        """
        Get the producer's config.

        :return:        Pydantic model with librdkafka producer's configuration.
        """
        return self._config

    # TODO (tribunsky.kir): rethink API?
    #                       https://github.com/severstal-digital/wunderkafka/issues/91
    # https://github.com/python/mypy/issues/13966
    def send_message(  # type: ignore[valid-type] # noqa: PLR0913
        self,
        topic: str,
        value: str | bytes | None = None,
        key: str | bytes | None = None,
        partition: int | None = None,
        on_delivery: DeliveryCallback | None = error_callback,
        *args: P.args,  # type: ignore[valid-type] # noqa: ARG002
        blocking: bool = False,
        **kwargs: P.kwargs,  # type: ignore[valid-type]
    ) -> None:
        if partition is not None:
            self.produce(topic, value, key=key, partition=partition, on_delivery=on_delivery, **kwargs)
        else:
            self.produce(topic, value, key=key, on_delivery=on_delivery, **kwargs)
        if blocking:
            self.flush()
        else:
            self.poll(0)
