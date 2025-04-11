from confluent_kafka import TopicPartition
from wunderkafka.config.generated import enums
from wunderkafka.config.rdkafka import ConsumerConfig, ProducerConfig
from wunderkafka.consumers.bytes import BytesConsumer
from wunderkafka.producers.bytes import BytesProducer
from wunderkafka.producers.transaction import EOSTransaction


def main():
    producer_config = ProducerConfig(
        bootstrap_servers='localhost:9092',
        transactional_id='eos_transaction_example',
    )
    consumer_config = ConsumerConfig(
        bootstrap_servers='localhost:9092',
        group_id='eos',
        auto_offset_reset=enums.AutoOffsetReset.earliest,
        # Do not advance committed offsets outside of the transaction.
        # Consumer offsets are committed along with the transaction
        # using the producer's send_offsets_to_transaction() API.
        # But don't care! We handle it for you!
        enable_auto_commit=False,  # this is important!
        enable_partition_eof=True,
    )

    producer = BytesProducer(producer_config)
    consumer = BytesConsumer(consumer_config)
    producer.prepare_transactions()
    consumer.assign([TopicPartition('input_topic', 0)])

    with EOSTransaction(producer, consumer):
        msgs = consumer.batch_poll()
        for msg in msgs:
            producer.send_message(
                'output_topic', 
                msg.value(),
                msg.key()
            )


if __name__ == "__main__":
    main()
