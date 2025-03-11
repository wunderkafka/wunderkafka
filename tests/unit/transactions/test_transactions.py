from unittest.mock import Mock, patch

from confluent_kafka import TopicPartition

from wunderkafka.producers.transaction import EOSTransaction
from wunderkafka.tests.consumer import TestConsumer
from wunderkafka.tests.producer import TestProducer


def test_transaction(topic: str, patched_producer: TestProducer):
    consumer = TestConsumer([])
    consumer.assignment = Mock()
    consumer.assignment.return_value = [TopicPartition(topic, 0)]
    consumer.position = Mock()
    consumer.position.return_value = 1
    consumer.consumer_group_metadata = Mock()
    consumer.consumer_group_metadata.return_value = 'fake_meta'

    with EOSTransaction(patched_producer, consumer): ...

    patched_producer.begin_transaction.assert_called_once()

    consumer.assignment.assert_called_once()
    consumer.position.assert_called_once()
    assert consumer.position.call_args.args == ([TopicPartition(topic, 0)],)
    consumer.consumer_group_metadata.assert_called_once()

    patched_producer.send_offsets_to_transaction.assert_called_once()
    assert patched_producer.send_offsets_to_transaction.call_args.args == (
        1, 'fake_meta'
    )

    patched_producer.commit_transaction.assert_called_once()


def test_transaction_no_consumer(patched_producer: TestProducer):

    with EOSTransaction(patched_producer):
        ...

    patched_producer.begin_transaction.assert_called_once()
    patched_producer.send_offsets_to_transaction.assert_not_called()
    patched_producer.commit_transaction.assert_called_once()


def test_transaction_with_raise(topic: str, patched_producer: TestProducer):
    consumer = TestConsumer([])
    consumer.assignment = Mock()
    consumer.assignment.return_value = [TopicPartition(topic, 0)]
    consumer.position = Mock()
    consumer.position.return_value = 1
    consumer.consumer_group_metadata = Mock()
    consumer.consumer_group_metadata.return_value = 'fake_meta'

    try:
        with EOSTransaction(patched_producer, consumer):
            raise Exception
    except: ...

    patched_producer.begin_transaction.assert_called_once()

    consumer.assignment.assert_not_called()
    consumer.position.assert_not_called()
    consumer.consumer_group_metadata.assert_not_called()

    patched_producer.send_offsets_to_transaction.assert_not_called()

    patched_producer.commit_transaction.assert_not_called()
    patched_producer.abort_transaction.assert_called_once()

