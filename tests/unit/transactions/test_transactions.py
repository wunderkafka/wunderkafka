from confluent_kafka import TopicPartition

from wunderkafka.consumers.constructor import HighLevelDeserializingConsumer
from wunderkafka.producers.constructor import HighLevelSerializingProducer
from wunderkafka.serdes.schemaless.json.deserializers import SchemaLessJSONDeserializer
from wunderkafka.serdes.schemaless.json.serializers import SchemaLessJSONSerializer
from wunderkafka.tests.consumer import TestConsumer
from wunderkafka.tests.producer import TestProducer
from wunderkafka.transactions import EOSTransaction


def test_transaction(topic: str, patched_producer: TestProducer, patched_consumer: TestConsumer) -> None:
    with EOSTransaction(patched_producer, patched_consumer): ...

    patched_producer.begin_transaction.assert_called_once()

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

    with EOSTransaction(patched_producer):
        ...

    patched_producer.begin_transaction.assert_called_once()
    patched_producer.send_offsets_to_transaction.assert_not_called()
    patched_producer.commit_transaction.assert_called_once()


def test_transaction_with_raise(patched_producer: TestProducer, patched_consumer: TestConsumer) -> None:
    try:
        with EOSTransaction(patched_producer, patched_consumer):
            raise Exception
    except: ...

    patched_producer.begin_transaction.assert_called_once()

    patched_consumer.assignment.assert_not_called()
    patched_consumer.position.assert_not_called()
    patched_consumer.consumer_group_metadata.assert_not_called()

    patched_producer.send_offsets_to_transaction.assert_not_called()

    patched_producer.commit_transaction.assert_not_called()
    patched_producer.abort_transaction.assert_called_once()


def test_serializing_consumer_producer(topic: str, patched_producer: TestProducer, patched_consumer: TestConsumer) -> None:
    producer = HighLevelSerializingProducer(
        patched_producer, 
        None, 
        None, 
        value_serializer=SchemaLessJSONSerializer(),
        key_serializer=SchemaLessJSONSerializer(),
    )
    consumer = HighLevelDeserializingConsumer(patched_consumer, None, None, deserializer=SchemaLessJSONDeserializer())

    with EOSTransaction(producer, consumer): ...


    patched_producer.begin_transaction.assert_called_once()

    patched_consumer.assignment.assert_called_once()
    patched_consumer.position.assert_called_once()
    assert patched_consumer.position.call_args.args == ([TopicPartition(topic, 0)],)
    patched_consumer.consumer_group_metadata.assert_called_once()

    patched_producer.send_offsets_to_transaction.assert_called_once()
    assert patched_producer.send_offsets_to_transaction.call_args.args == (
        [TopicPartition(topic, 0, 1)], 'fake_meta', None
    )

    patched_producer.commit_transaction.assert_called_once()

