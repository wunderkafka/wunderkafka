from unittest.mock import Mock

import pytest
from confluent_kafka import TopicPartition

from wunderkafka.tests.consumer import TestConsumer
from wunderkafka.tests.producer import TestProducer


@pytest.fixture
def patched_producer() -> TestProducer:
    p = TestProducer()
    p.poll = Mock()
    return p


@pytest.fixture 
def patched_consumer(topic: str) -> TestConsumer:
    c = TestConsumer([])
    c.assignment.return_value = [TopicPartition(topic, 0)]
    c.position.return_value = [TopicPartition(topic, 0, 1)]
    c.consumer_group_metadata.return_value = 'fake_meta'

    return c
