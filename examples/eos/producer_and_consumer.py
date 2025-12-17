from confluent_kafka import TopicPartition
from wunderkafka.config.rdkafka import ConsumerConfig, ProducerConfig
from wunderkafka.consumers.bytes import BytesConsumer
from wunderkafka.eos import EOSTransaction
from wunderkafka.producers.bytes import BytesProducer


def main():
    producer_config = ProducerConfig(
        bootstrap_servers='localhost:9092',
        # Define a transactional ID for the producer to enable EOS
        transactional_id='eos_transaction_example',
        # transaction.max.timeout.ms must be greater than or equal to message.timeout.ms
        transaction_timeout_ms = 300000,
        message_timeout_ms=300000,
    )
    consumer_config = ConsumerConfig(
        bootstrap_servers='localhost:9092',
        group_id='eos',
        # Do not advance committed offsets outside of the transaction.
        # Consumer offsets are committed along with the transaction
        # using the producer's send_offsets_to_transaction() API.
        enable_auto_commit=False,  # this is important!
    )

    producer = BytesProducer(producer_config)
    consumer = BytesConsumer(consumer_config)
    consumer.assign([TopicPartition('input_topic', 0)])

    with EOSTransaction(producer, consumer):
        msgs = consumer.batch_poll()
        for msg in msgs:
            producer.send_message('output_topic', msg.value(), msg.key())


if __name__ == "__main__":
    main()
