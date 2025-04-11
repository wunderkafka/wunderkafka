import pytest
from confluent_kafka import TopicPartition

from wunderkafka.consumers.constructor import HighLevelDeserializingConsumer
from wunderkafka.producers.constructor import HighLevelSerializingProducer
from wunderkafka.serdes.schemaless.string.deserializers import StringDeserializer
from wunderkafka.serdes.schemaless.string.serializers import StringSerializer
from wunderkafka.tests.consumer import TestConsumer
from wunderkafka.tests.producer import TestProducer
from wunderkafka.transactions import EOSTransaction


def test_transaction_without_prepare_transactions(topic: str, patched_producer: TestProducer, patched_consumer: TestConsumer) -> None:
    with pytest.raises(AssertionError):
        EOSTransaction(patched_producer, patched_consumer)


def test_transaction(topic: str, patched_producer: TestProducer, patched_consumer: TestConsumer) -> None:
    patched_producer.prepare_transactions()
    with EOSTransaction(patched_producer, patched_consumer): ...

    patched_producer.begin_transaction.assert_called_once()
    patched_producer.poll.assert_called_once()

    patched_consumer.assignment.assert_called_once()
    patched_consumer.position.assert_called_once()
    assert patched_consumer.position.call_args.args == ([TopicPartition(topic, 0)],)
    patched_consumer.consumer_group_metadata.assert_called_once()

    patched_producer.send_offsets_to_transaction.assert_called_once()
    assert patched_producer.send_offsets_to_transaction.call_args.args == (
        [TopicPartition(topic, 0, 1)], 'fake_meta'
    )

    patched_producer.commit_transaction.assert_called_once()


def test_transaction_no_consumer(patched_producer: TestProducer) -> None:
    patched_producer.prepare_transactions()

    with EOSTransaction(patched_producer):
        ...

    patched_producer.begin_transaction.assert_called_once()
    patched_producer.poll.assert_called_once()

    patched_producer.send_offsets_to_transaction.assert_not_called()
    patched_producer.commit_transaction.assert_called_once()


def test_transaction_with_raise(patched_producer: TestProducer, patched_consumer: TestConsumer) -> None:
    patched_producer.prepare_transactions()
    try:
        with EOSTransaction(patched_producer, patched_consumer):
            raise Exception
    except: ...

    patched_producer.begin_transaction.assert_called_once()
    patched_producer.poll.assert_called_once()

    patched_consumer.assignment.assert_not_called()
    patched_consumer.position.assert_not_called()
    patched_consumer.consumer_group_metadata.assert_not_called()

    patched_producer.send_offsets_to_transaction.assert_not_called()

    patched_producer.commit_transaction.assert_not_called()
    patched_producer.abort_transaction.assert_called_once()


def test_serializing_consumer_producer(topic: str, patched_producer: TestProducer, patched_consumer: TestConsumer) -> None:
    patched_producer.prepare_transactions()
    producer = HighLevelSerializingProducer(
        patched_producer,
        None,
        None,
        value_serializer=StringSerializer(),
        key_serializer=StringSerializer(),
    )
    consumer = HighLevelDeserializingConsumer(patched_consumer, None, None, deserializer=StringDeserializer())

    with EOSTransaction(producer.producer, consumer.consumer): ...


    patched_producer.begin_transaction.assert_called_once()
    patched_producer.poll.assert_called_once()

    patched_consumer.assignment.assert_called_once()
    patched_consumer.position.assert_called_once()
    assert patched_consumer.position.call_args.args == ([TopicPartition(topic, 0)],)
    patched_consumer.consumer_group_metadata.assert_called_once()

    patched_producer.send_offsets_to_transaction.assert_called_once()
    assert patched_producer.send_offsets_to_transaction.call_args.args == (
        [TopicPartition(topic, 0, 1)], 'fake_meta'
    )

    patched_producer.commit_transaction.assert_called_once()

