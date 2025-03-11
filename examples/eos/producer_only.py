from wunderkafka.config.rdkafka import ProducerConfig
from wunderkafka.producers.bytes import BytesProducer
from wunderkafka.producers.transaction import EOSTransaction


def main():
    producer_config = ProducerConfig(
        bootstrap_servers='kafka-broker-01.my_domain.com:9093',
        transactional_id='eos_transaction_example'
    )
    producer = BytesProducer(producer_config)

    msgs = [(f'value: {i}', i) for i in range(100)]

    with EOSTransaction(producer):
        for value, key in msgs:
            producer.send_message(
                'output_topic', 
                value,
                key
            )


if __name__ == "__main__":
    main()
