"""This module contains implementation of extended confluent-kafka Consumer's API."""

from __future__ import annotations

import time
import atexit
import datetime

from confluent_kafka import KafkaException

from wunderkafka.types import HowToSubscribe
from wunderkafka.config import ConsumerConfig
from wunderkafka.errors import ConsumerException
from wunderkafka.logger import logger
from wunderkafka.callbacks import reset_partitions
from wunderkafka.consumers.abc import Message, AbstractConsumer
from wunderkafka.config.krb.rdkafka import challenge_krb_arg
from wunderkafka.consumers.subscription import TopicSubscription


class BytesConsumer(AbstractConsumer):
    """Consumer implementation of extended interface for raw messages."""

    def __init__(self, config: ConsumerConfig) -> None:
        """
        Init consumer.

        :param config:          Pydantic BaseSettings model with librdkafka consumer's configuration.
        """
        try:
            super().__init__(config.dict())
        except KafkaException as exc:
            config = challenge_krb_arg(exc, config)
            super().__init__(config.dict())
        self.subscription_offsets: dict[str, HowToSubscribe] | None = None

        self._config = config
        self._last_poll_ts = time.perf_counter()
        # TODO (tribunsky-kir): make it configurable
        #                       https://github.com/severstal-digital/wunderkafka/issues/90
        atexit.register(self.close)

    def __str__(self) -> str:
        """
        Get human-readable representation of consumer.

        :return:    string with consumer gid.
        """
        return f"{self.__class__.__name__}:{self._config.group_id}"

    def batch_poll(
        self,
        timeout: float = 1.0,
        num_messages: int = 1000000,
        *,
        raise_on_lost: bool = False,
    ) -> list[Message]:
        # TODO (tribunsky.kir): maybe it better to use on_lost callback within subscribe()
        #                       https://github.com/severstal-digital/wunderkafka/issues/95
        dt = int((time.perf_counter() - self._last_poll_ts) * 1000)
        if dt > self._config.max_poll_interval_ms:
            msg = f"Exceeded max.poll.interval.ms ({self._config.max_poll_interval_ms}): {dt}"

            if raise_on_lost:
                # TODO (tribunsky.kir): resubscribe by ts?
                #                       https://github.com/severstal-digital/wunderkafka/issues/95
                raise ConsumerException(msg)
            logger.warning(msg)

        msgs = self.consume(num_messages=num_messages, timeout=timeout)
        self._last_poll_ts = time.perf_counter()
        return msgs

    # TODO (tribunsky.kir): do not override original API and wrap it in superclass
    #                       https://github.com/severstal-digital/wunderkafka/issues/93
    def subscribe(  # noqa: PLR0913
        self,
        topics: list[str | TopicSubscription],
        *,
        from_beginning: bool | None = None,
        offset: int | None = None,
        ts: float | None = None,
        dt: datetime.datetime | None = None,
        with_timedelta: datetime.timedelta | None = None,
    ) -> None:
        """
        Subscribe to a list of topics, or a list of specific TopicSubscriptions.

        This method overrides original `subscribe()` method of `confluent-kafka.Consumer` and allows to subscribe
        to a topic via specific offset or timestamp.

        Doc of an original method depends on python's :code:`confluent-kafka` version. Please, refer to
        `confluent-kafka documentation <https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#pythonclient-consumer>`__.

        .. warning::
            Currently this method doesn't allow passing callbacks and uses its own to reset partitions.
        """
        subscriptions = {}
        for tpc in topics:
            if isinstance(tpc, str):
                tpcs = TopicSubscription(
                    topic=tpc,
                    from_beginning=from_beginning,
                    offset=offset,
                    ts=ts,
                    dt=dt,
                    with_timedelta=with_timedelta,
                )
            else:
                tpcs = tpc
            subscriptions[tpcs.topic] = tpcs.how

        # We have a specific subscription at least once
        if any(subscriptions.values()):
            self.subscription_offsets = subscriptions
            # TODO (tribunsky.kir): avoid mutation of self.subscription_offset and remove it as a field
            #                       https://github.com/severstal-digital/wunderkafka/issues/87
            super().subscribe(topics=list(self.subscription_offsets), on_assign=reset_partitions)
        else:
            super().subscribe(topics=list(subscriptions))
