"""
The Module contains interface-like skeletons for consumer.

- Inherited from confluent-kafka Consumer to use it as nested entity
- high-level consumer which is able to handle message's schemas
"""

from __future__ import annotations

import datetime
from abc import ABC, abstractmethod

from confluent_kafka import Message, Consumer, TopicPartition

from wunderkafka.types import HowToSubscribe
from wunderkafka.config import ConsumerConfig
from wunderkafka.consumers.subscription import TopicSubscription


class AbstractConsumer(Consumer):
    """Extension point for the original Consumer API."""

    # Why so: https://github.com/python/mypy/issues/4125
    _config: ConsumerConfig
    subscription_offsets: dict[str, HowToSubscribe] | None = None

    # TODO (tribunsky.kir): Do we need re-initiation of consumer/producer in runtime?
    #                       https://github.com/severstal-digital/wunderkafka/issues/94
    @property
    def config(self) -> ConsumerConfig:
        """
        Get the consumer's config.

        Tech Debt: needed here for specific callback which resets partition to specific offset.

        :return:        Pydantic model with librdkafka consumer's configuration.
        """
        return self._config

    def batch_poll(
        self,
        timeout: float = 1.0,
        num_messages: int = 1000000,
        *,
        raise_on_lost: bool = False,
    ) -> list[Message]:
        """
        Consume as many messages as we can for a given timeout.

        Created to allow deserializing consumer nest BytesConsumer and read messages via batches
        and not communicate with broker for every single message.

        :param timeout:         The maximum time to block waiting for message.
        :param num_messages:    The maximum number of messages to receive from broker.
                                Default is 1000000 which was the allowed maximum for librdkafka 1.2.
        :param raise_on_lost:   If True, raise exception whenever we are too late to call next poll.
                                Otherwise, will do nothing or behave depending on specified callbacks.

        :return:                A list of Message objects (possibly empty on timeout).

        ..  # noqa: DAR401
        ..  # noqa: DAR202
        """
        raise NotImplementedError


class AbstractDeserializingConsumer(ABC):
    """High-level interface for extended consumer."""

    @abstractmethod
    def commit(
        self,
        message: Message | None = None,
        offsets: list[TopicPartition] | None = None,
        # TODO (tribunsky.kir): implement API to allow only keyword arguments for booleans
        #                       https://github.com/severstal-digital/wunderkafka/issues/93
        asynchronous: bool = True,  # noqa: FBT001, FBT002
    ) -> list[TopicPartition] | None:
        """
        Commit a message or a list of offsets.

        This method overlaps the original consumer's method and will use the nested consumer.

        :param message:         Commit offset (+1), extracted from Message object itself.
        :param offsets:         Commit exactly TopicPartition data.
        :param asynchronous:    If True, do not block execution, otherwise - wait until commit fail or success.

        :raises KafkaException: If all commits failed.

        :return:                On asynchronous call returns None immediately.
                                Committed offsets on synchronous call, if succeeded.
        """

    # TODO (tribunsky.kir): reconsider API of 'how'
    #                       https://github.com/severstal-digital/wunderkafka/issues/89
    @abstractmethod
    def subscribe(
        self,
        topics: list[str | TopicSubscription],
        *,
        from_beginning: bool | None = None,
        offset: int | None = None,
        ts: int | None = None,
        with_timedelta: datetime.timedelta | None = None,
    ) -> None:
        """
        Subscribe to a given list of topics. This replaces a previous subscription.

        This method overlaps the original consumer's method and will use the nested consumer.

        :param topics:          List of topics to subscribe to. If a topic has no specific subscription, specified value
                                for beginning/end/offset/timestamp/timedelta/built-in will be used.
        :param from_beginning:  If a flag is set,
                                return specific offset corresponding to the beginning/end of the topic.
        :param offset:          If set, will return an Offset object.
        :param ts:              If set, will return Timestamp.
        :param with_timedelta:  If set, will calculate timestamp for corresponding timedelta.
        """
