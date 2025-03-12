from typing import Generator
from unittest.mock import Mock

import pytest
from confluent_kafka import TopicPartition

from wunderkafka.tests.consumer import TestConsumer
from wunderkafka.tests.producer import TestProducer


@pytest.fixture
def patched_producer() -> Generator[TestProducer, None, None]:
    p = TestProducer()
    p.init_transactions = Mock()
    p.begin_transaction = Mock()
    p.commit_transaction = Mock()
    p.abort_transaction = Mock()
    p.send_offsets_to_transaction = Mock()
    return p


@pytest.fixture
def patched_consumer(topic: str) -> Generator[TestConsumer, None, None]:
    c = TestConsumer([])
    c.assignment = Mock()
    c.assignment.return_value = [TopicPartition(topic, 0)]
    c.position = Mock()
    c.position.return_value = [TopicPartition(topic, 0, 1)]
    c.consumer_group_metadata = Mock()
    c.consumer_group_metadata.return_value = 'fake_meta'

    return c
