from typing import Generator
from unittest.mock import Mock, patch

import pytest

from wunderkafka.tests.producer import TestProducer


@pytest.fixture
def patched_producer() -> Generator[TestProducer, None, None]:
    p = TestProducer()
    p.init_transactions = Mock()
    p.begin_transaction = Mock()
    p.commit_transaction = Mock()
    p.abort_transaction = Mock()
    p.send_offsets_to_transaction = Mock()
    return p

