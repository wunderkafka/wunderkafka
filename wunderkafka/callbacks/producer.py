"""This module contains some predefined callbacks to interact with librdkafka."""

from __future__ import annotations

from confluent_kafka import Message, KafkaError

from wunderkafka.logger import logger


def info_callback(err: KafkaError | None, msg: Message) -> None:
    """
    Log every message delivery.

    :param err:             Error, if any, thrown from confluent-kafka cimpl.
    :param msg:             Message to be delivered.
    """
    if err is None:
        logger.info(f"Message delivered to {msg.topic()} partition: {msg.partition()}")
    else:
        logger.error(f"Message failed delivery: {err}")


def error_callback(err: KafkaError | None, _: Message) -> None:
    """
    Log only failed message delivery.

    :param err:             Error, if any, thrown from confluent-kafka cimpl.
    :param _:               Message to be delivered (unused, but needed to not break callback signature).
    """
    if err:
        logger.error(f"Message failed delivery: {err}")
