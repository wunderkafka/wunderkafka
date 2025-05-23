"""This module contains some predefined callbacks to interact with librdkafka."""

from __future__ import annotations

from confluent_kafka import TopicPartition

from wunderkafka.logger import logger
from wunderkafka.structures import Timestamp
from wunderkafka.consumers.abc import AbstractConsumer


def reset_partitions(consumer: AbstractConsumer, partitions: list[TopicPartition]) -> None:
    """
    Set a specific offset for assignment after subscription.

    Depending on the type of subscription, will set offset or timestamp.

    :param consumer:            Consumer, which is subscribed to topics.
    :param partitions:          List of TopicPartitions, which is returned from the underlying library.
    """
    new_offsets = consumer.subscription_offsets
    if new_offsets is None:
        logger.warning(
            f"{consumer}: re-assigned (using auto.offset.reset={consumer.config.auto_offset_reset})",
        )
        return
    by_offset = []
    by_ts = []
    for partition in partitions:
        new_offset = new_offsets[partition.topic]
        if new_offset is None:
            by_offset.append(partition)
        else:
            partition.offset = new_offset.value
            if isinstance(new_offset, Timestamp):
                logger.info(f"Setting {new_offset}...")
                by_ts.append(partition)
            else:
                by_offset.append(partition)
    if by_ts:
        by_ts = consumer.offsets_for_times(by_ts)
    new_partitions = by_ts + by_offset
    consumer.assign(new_partitions)
    logger.info(f"{consumer} assigned to {new_partitions}")
    consumer.subscription_offsets = None
