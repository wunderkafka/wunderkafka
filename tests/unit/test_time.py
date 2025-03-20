import time
import datetime

import pytest

from wunderkafka.time import ts2dt


def test_invalid_ts() -> None:
    with pytest.raises(ValueError, match="seems not to be a UNIX-timestamp in seconds or milliseconds"):
        ts2dt(time.time() * 1000000)


def test_seconds() -> None:
    dt = datetime.datetime.now(datetime.timezone.utc)
    assert ts2dt(dt.timestamp()) == dt


def test_ms() -> None:
    dt = datetime.datetime.now(datetime.timezone.utc)
    assert ts2dt(dt.timestamp() * 1000) - dt.replace(microsecond=0) <= datetime.timedelta(seconds=1)
