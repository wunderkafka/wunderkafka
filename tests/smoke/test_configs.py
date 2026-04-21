import time
import logging
from typing import Any

import pytest
from pydantic import ValidationError

from wunderkafka import SRConfig, BytesConsumer, BytesProducer, ConsumerConfig, ProducerConfig, SecurityProtocol
from tests.smoke.conftest import RawConfig


def test_init_consumer(boostrap_servers: str) -> None:
    config = ConsumerConfig(group_id="my_group", bootstrap_servers=boostrap_servers)
    consumer = BytesConsumer(config)
    print(consumer)

    with pytest.raises(AttributeError):
        consumer.config = ConsumerConfig(  # type: ignore
            group_id="my_other_group",
            bootstrap_servers=boostrap_servers,
        )
    assert consumer.config == config

    consumer.close()


def test_init_producer(boostrap_servers: str) -> None:
    config = ProducerConfig(bootstrap_servers=boostrap_servers)
    BytesProducer(config)


def test_init_no_krb_consumer(non_krb_config: RawConfig) -> None:
    config = ConsumerConfig(group_id="my_group", **non_krb_config)
    consumer = BytesConsumer(config)
    print(consumer)

    with pytest.raises(AttributeError):
        consumer.config = ConsumerConfig(  # type: ignore
            group_id="my_other_group",
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


# confluent-kafka >= 2.13.0 blocks Consumer/Producer.__init__() in wait_for_oauth_token_set()
# until the oauth_cb is invoked and rd_kafka_oauthbearer_set_token() succeeds. librdkafka only
# invokes the refresh callback when SASL/OAUTHBEARER is the active mechanism — tests using
# oauth_cb must therefore set security_protocol=sasl_plaintext and sasl_mechanism=OAUTHBEARER,
# and the callback must return a non-empty token with a future expiry.
def dummy_oauth_cb(_: Any) -> tuple:
    return "dummy-token", time.time() + 3600


def test_init_consumer_oauth_cb(boostrap_servers: str) -> None:
    config = ConsumerConfig(
        group_id="my_group",
        bootstrap_servers=boostrap_servers,
        oauth_cb=dummy_oauth_cb,
        security_protocol=SecurityProtocol.sasl_plaintext,
        sasl_mechanism="OAUTHBEARER",
    )
    consumer = BytesConsumer(config)
    print(consumer)

    with pytest.raises(AttributeError):
        consumer.config = ConsumerConfig(  # type: ignore
            group_id="my_other_group",
            bootstrap_servers=boostrap_servers,
        )
    assert consumer.config == config

    consumer.close()


def test_init_producer_oauth_cb(boostrap_servers: str) -> None:
    config = ProducerConfig(
        bootstrap_servers=boostrap_servers,
        oauth_cb=dummy_oauth_cb,
        security_protocol=SecurityProtocol.sasl_plaintext,
        sasl_mechanism="OAUTHBEARER",
    )
    BytesProducer(config)


def test_init_consumer_log_cb(boostrap_servers: str) -> None:
    mylogger = logging.getLogger()
    mylogger.addHandler(logging.StreamHandler())
    config = ConsumerConfig(
        group_id="my_group",
        bootstrap_servers=boostrap_servers,
        oauth_cb=dummy_oauth_cb,
        logger=mylogger,
        security_protocol=SecurityProtocol.sasl_plaintext,
        sasl_mechanism="OAUTHBEARER",
    )
    consumer = BytesConsumer(config)
    print(consumer)

    with pytest.raises(AttributeError):
        consumer.config = ConsumerConfig(  # type: ignore
            group_id="my_other_group",
            bootstrap_servers=boostrap_servers,
        )
    assert consumer.config == config

    consumer.close()


def test_init_producer_log_cb(boostrap_servers: str) -> None:
    mylogger = logging.getLogger()
    mylogger.addHandler(logging.StreamHandler())
    config = ProducerConfig(
        bootstrap_servers=boostrap_servers,
        oauth_cb=dummy_oauth_cb,
        logger=mylogger,
        security_protocol=SecurityProtocol.sasl_plaintext,
        sasl_mechanism="OAUTHBEARER",
    )
    BytesProducer(config)
