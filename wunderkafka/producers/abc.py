"""
Module contains interface-like skeletons for producer.

- Inherited from confluent-kafka Producer to use it as nested entity
- high-level producer which is able to handle message's schemas
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TypeVar

from confluent_kafka import Producer, TopicPartition

from wunderkafka.types import MsgKey, MsgValue, DeliveryCallback
from wunderkafka.compat import ParamSpec

P = ParamSpec("P")
T = TypeVar("T")


class AbstractProducer(Producer):
    """Extension point for the original Producer API."""

    @property
    def transaction_ready(self) -> bool:
        """Returns True if init_transactions() has already been called and False otherwise."""
        return getattr(self, "__transaction_ready", False) is True

    def prepare_transactions(self) -> None:
        """Call init_transactions() and set internal flag to indicate that it has been called."""
        self.init_transactions()
        setattr(self, "__transaction_ready", True)

    def start_transaction(self, poll_timeout: float = 0.0) -> None:
        """Call begin_transaction() to start a new transaction and poll() to trigger producer's events."""
        self.begin_transaction()
        self.poll(poll_timeout)

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

    @property
    @abstractmethod
    def transaction_ready(self) -> bool:
        """Return True if init_transactions() has already been called and False otherwise."""

    @abstractmethod
    def prepare_transactions(self) -> None:
        """Call init_transactions() and set internal flag to indicate that it has been called."""

    @abstractmethod
    def start_transaction(self, poll_timeout: float = 0.0) -> None:
        """Call begin_transaction() to start a new transaction and poll() to trigger producer's events."""

    @abstractmethod
    def commit_transaction(self, timeout: float | None = None) -> None:
        """
        Commmit the current transaction.

        This method overlaps the original producers' method and will use the nested producer.

        Any outstanding messages will be flushed (delivered) before actually committing the transaction.

        If any of the outstanding messages fail permanently the current transaction will enter the abortable error
        state and this function will return an abortable error, in this case the application must call
        abort_transaction() before attempting a new transaction with begin_transaction().

        Note: This function will block until all outstanding messages are delivered and the transaction commit request
        has been successfully handled by the transaction coordinator, or until the timeout expires, which ever comes
        first. On timeout the application may call the function again.

        Note: Will automatically call flush() to ensure all queued messages are delivered before attempting to commit
        the transaction. Delivery reports and other callbacks may thus be triggered from this method.

        :param timeout:         The amount of time to block in seconds.

        :raises KafkaError:     Use exc.args[0].retriable() to check if the operation may be retried, or
                                exc.args[0].txn_requires_abort() if the current  transaction has failed and must be
                                aborted by calling abort_transaction() and then start a new transaction
                                with begin_transaction().
                                Treat any other error as a fatal error.
        """

    def abort_transaction(self, timeout: float | None = None) -> None:
        """
        Aborts the current transaction.

        This method overlaps the original producers' method and will use the nested producer.

        This function should also be used to recover from non-fatal abortable transaction errors when
        KafkaError.txn_requires_abort() is True.

        Any outstanding messages will be purged and fail with _PURGE_INFLIGHT or _PURGE_QUEUE.

        Note: This function will block until all outstanding messages are purged and the transaction abort request
        has been successfully handled by the transaction coordinator, or until the timeout expires, which ever comes
        first. On timeout the application may call the function again.

        Note: Will automatically call purge() and flush() to ensure all queued and in-flight messages are purged before
        attempting to abort the transaction.

        :param timeout:         The maximum amount of time to block waiting for transaction to abort in seconds.

        :raises KafkaError:     Use exc.args[0].retriable() to check if the operation may be retried.
                                Treat any other error as a fatal error.
        """

    @abstractmethod
    def send_offsets_to_transaction(
        self, offsets: list[TopicPartition], group_metadata: object, timeout: float | None = None
    ) -> None:  # type: ignore[valid-type]
        """
        This method overlaps the original producers' method and will use the nested producer.

        Sends a list of topic partition offsets to the consumer group coordinator for group_metadata and
        marks the offsets as part of the current transaction.
        These offsets will be considered committed only if the transaction is committed successfully.

        The offsets should be the next message your application will consume, i.e., the last processed
        message's offset + 1 for each partition.
        Either track the offsets manually during processing or use consumer.position() (on the consumer) to get
        the current offsets for the partitions assigned to the consumer.

        Use this method at the end of a consume-transform-produce loop prior to committing the transaction with
        commit_transaction().

        Note: The consumer must disable auto commits (set `enable.auto.commit` to false on the consumer).

        Note: Logical and invalid offsets (e.g., OFFSET_INVALID) in offsets will be ignored.
        If there are no valid offsets in offsets the function will return successfully and no action will be taken.

        :param offsets:         current consumer/processing position(offsets) for the list of partitions.
        :param group_metadata:  consumer group metadata retrieved from the input consumer's
                                get_consumer_group_metadata().
        :param timeout:         Amount of time to block in seconds.

        :raises KafkaError:     Use exc.args[0].retriable() to check if the operation may be retried, or
                                exc.args[0].txn_requires_abort() if the current transaction has failed and must be
                                aborted by calling abort_transaction() and then start a new transaction
                                with begin_transaction().
                                Treat any other error as a fatal error.
        """

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
