from wunderkafka.config.rdkafka import ProducerConfig
from wunderkafka.eos import EOSTransaction
from wunderkafka.producers.bytes import BytesProducer


def main():
    producer_config = ProducerConfig(
        bootstrap_servers='localhost:9092',
        allow_auto_create_topics=True,
        # Define a transactional ID for the producer to enable EOS
        transactional_id='eos_transaction_example',
        # transaction.max.timeout.ms must be greater than or equal to message.timeout.ms
        transaction_timeout_ms = 300000,
        message_timeout_ms=300000,
    )
    producer = BytesProducer(producer_config)

    msgs = ((str(i), str(i)) for i in range(100))
    with EOSTransaction(producer):
        for key, value in msgs:
            print(f'sending: {value} | {key}')
            producer.send_message('output_topic', value, key)

    print('done')


if __name__ == "__main__":
    main()
