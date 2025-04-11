from wunderkafka.config.rdkafka import ProducerConfig
from wunderkafka.producers.bytes import BytesProducer
from wunderkafka.producers.transaction import EOSTransaction


def main():
    producer_config = ProducerConfig(
        bootstrap_servers='localhost:9092',
        transactional_id='eos_transaction_example',
        allow_auto_create_topics=True,
    )
    producer = BytesProducer(producer_config)
    producer.prepare_transactions()

    msgs = [(f'value: {i}', i) for i in range(100)]

    with EOSTransaction(producer):
        for value, key in msgs:
            print(f'sending: {value} | {key}')
            producer.send_message(
                'output_topic', 
                value.encode(),
                bytes(key),
            )

    print('done')


if __name__ == "__main__":
    main()
