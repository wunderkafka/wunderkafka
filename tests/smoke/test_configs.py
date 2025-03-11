import pytest
from pydantic import ValidationError
from dirty_equals import IsPartialDict

from tests.smoke.conftest import RawConfig
from wunderkafka import BytesConsumer, BytesProducer, ConsumerConfig, ProducerConfig, SRConfig


def test_init_consumer(boostrap_servers: str) -> None:
    config = ConsumerConfig(group_id='my_group', bootstrap_servers=boostrap_servers)
    consumer = BytesConsumer(config)
    print(consumer)

    with pytest.raises(AttributeError):
        consumer.config = ConsumerConfig(                                                                 # type: ignore
            group_id='my_other_group',
            bootstrap_servers=boostrap_servers,
        )
    assert consumer.config == config

    consumer.close()


def test_init_producer(boostrap_servers: str) -> None:
    config = ProducerConfig(bootstrap_servers=boostrap_servers)
    BytesProducer(config)


def test_init_no_krb_consumer(non_krb_config: RawConfig) -> None:
    config = ConsumerConfig(group_id='my_group', **non_krb_config)
    consumer = BytesConsumer(config)
    print(consumer)

    with pytest.raises(AttributeError):
        consumer.config = ConsumerConfig(                                                                 # type: ignore
            group_id='my_other_group',
            **non_krb_config,
        )
    assert consumer.config == config

    consumer.close()


def test_init_no_krb_producer(non_krb_config: RawConfig) -> None:
    config = ProducerConfig(**non_krb_config)
    BytesProducer(config)


def test_sr_required_url() -> None:
    with pytest.raises(ValidationError):
        SRConfig()


def test_group_id_required() -> None:
    with pytest.raises(ValidationError):
        ConsumerConfig()


@pytest.mark.parametrize(
    ('cfg_kwargs', 'expected_config'),
    [
        (
            {'transactional_id': 'test'},
            {
                'transactional.id': 'test',
                'enable.idempotence': True,
                'max.in.flight': 5,
                'max.in.flight.requests.per.connection': 5,
                'message.timeout.ms': 60_000,

            }
        ),
        (
            {
                'transactional_id': 'test',
                'max_in_flight': 3,
            },
            {
                'transactional.id': 'test',
                'enable.idempotence': True,
                'max.in.flight': 3,
                'max.in.flight.requests.per.connection': 3,
                'message.timeout.ms': 60_000,

            }
        ),
        (
            {
                'transactional_id': 'test',
                'message_timeout_ms': 1_000,
            },
            {
                'transactional.id': 'test',
                'enable.idempotence': True,
                'max.in.flight': 5,
                'max.in.flight.requests.per.connection': 5,
                'message.timeout.ms': 1_000,

            }
        )
    ]
)
def test_config_with_transactional_id(cfg_kwargs: dict, expected_config: dict) -> None:
    cfg = ProducerConfig(**cfg_kwargs)

    assert cfg.dict() == IsPartialDict(expected_config)
