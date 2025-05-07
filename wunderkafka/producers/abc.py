"""
Module contains interface-like skeletons for producer.

- Inherited from confluent-kafka Producer to use it as nested entity
- high-level producer which is able to handle message's schemas
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TypeVar

from confluent_kafka import Producer

from wunderkafka.types import MsgKey, MsgValue, DeliveryCallback
from wunderkafka.compat import ParamSpec

P = ParamSpec("P")
T = TypeVar("T")


class AbstractProducer(Producer):
    """Extension point for the original Producer API."""

    # TODO (tribunsky.kir): rethink API?
    #                       https://github.com/severstal-digital/wunderkafka/issues/91
    # https://github.com/python/mypy/issues/13966
    def send_message(  # noqa: PLR0913
        self,
        topic: str,
        value: str | bytes | None = None,
        key: str | bytes | None = None,
        partition: int | None = None,
        on_delivery: DeliveryCallback | None = None,
        *args: P.args,  # type: ignore[valid-type]
        blocking: bool = False,
        **kwargs: P.kwargs,  # type: ignore[valid-type]
    ) -> None:
        """
        Send an encoded message to Kafka almost immediately.

        This method overlaps the original producers' method.

        :param topic:           Target topic against which we are working for this call.
        :param value:           Message's value encoded object to be sent.
        :param key:             Message's key encoded object to be sent.
        :param partition:       Target partition to produce to. If none, uses configured built-in
                                partitioner by default.
        :param on_delivery:     Callback to be executed on successful/failed delivery.
        :param args:            Other positional arguments to call with original produce() API.
        :param blocking:        If True, will block execution until a delivery result is retrieved
                                (calls flush() under the hood). Otherwise, will produce a message to
                                builtin queue and call poll() to trigger producer's events.
        :param kwargs:          Other keyword arguments to call with original produce() API.
        """
        raise NotImplementedError


class AbstractSerializingProducer(ABC):
    """High-level interface for extended producer."""

    @abstractmethod
    # https://github.com/python/mypy/issues/13966
    def send_message(  # noqa: PLR0913
        self,
        topic: str,
        value: MsgValue = None,
        key: MsgKey = None,
        partition: int | None = None,
        on_delivery: DeliveryCallback | None = None,
        *args: P.args,  # type: ignore[valid-type]
        blocking: bool = False,
        **kwargs: P.kwargs,  # type: ignore[valid-type]
    ) -> None:
        """
        Encode and send a message to Kafka.

        This method overlaps AbstractProducer's method and intended to use produce() of real nested producer.

        :param topic:           Target topic against which we are working for this call.
        :param value:           Message's value object to be encoded and sent.
        :param key:             Message's key object to be encoded and sent.
        :param partition:       Target partition to produce to. If none, uses configured built-in
                                partitioner by default.
        :param on_delivery:     Callback to be executed on successful/failed delivery.
        :param args:            Other positional arguments to call with original produce() API.
        :param blocking:        If True, will block execution until a delivery result is retrieved
                                (calls flush() under the hood). Otherwise, will produce a message to
                                builtin queue and call poll() to trigger producer's events.
        :param kwargs:          Other keyword arguments to call with original produce() API.
        """

    # TODO (tribunsky.kir): change naming. It is like subscribe, but publish/add_topic whatever
    #                       https://github.com/severstal-digital/wunderkafka/issues/92
    @abstractmethod
    def set_target_topic(self, topic: str, value: T, key: T | None = None, *, lazy: bool = False) -> None:
        """
        Make producer aware how it should work with a specific topic.

        This method is used to instantiate producer with mapping, but also adds schemas in runtime.

        :param topic:           Target topic to specify schema description against.
        :param value:           Message's value schema description.
        :param key:             Message's key schema description.
        :param lazy:            If True, do not register schema in registry during __init__,
                                or before the first attempt to send a message for a given topic.
                                Otherwise, register schema immediately.
        """

    @abstractmethod
    def flush(self, timeout: float) -> int:
        """
        Wait for all messages in the Producer queue to be delivered.

        This method overlaps the original producers' method and will use the nested producer.

        :param timeout:         Maximum time to block.
        :return:                Number of messages still in queue.
        """
