"""Some timing boilerplate."""

import time
import datetime

from wunderkafka.logger import logger


def now() -> int:
    """
    Return UNIX timestamp in ms.

    :returns:        current timestamp (ms)
    """
    return int(time.time() * 1000)


def ts2dt(ts: float, tz: datetime.tzinfo = datetime.timezone.utc) -> datetime.datetime:
    """
    Convert unix-timestamp to a datetime object.

    This function is primarily intended for formatting timestamps for human readability,
    such as in logs or UI output, rather than for precise timestamp comparisons or decisions.

    :param ts:              Timestamp in seconds or milliseconds.
    :param tz:              Datetime object to use for conversion.
    :returns:               Corresponding datetime object.

    :raises ValueError:     Raised when couldn't coerce float to datetime.
    """
    ts_str_len_ms = 13  # 1559574586123
    ts_str_len_s = 10  # 155957458
    current_ts_len = len(str(int(ts)))
    if current_ts_len <= ts_str_len_s:
        return datetime.datetime.fromtimestamp(ts, tz)
    if current_ts_len == ts_str_len_ms:
        return datetime.datetime.fromtimestamp(round(ts / 1000), tz)
    msg = f"Invalid timestamp: {ts} seems not to be a UNIX-timestamp in seconds or milliseconds"
    raise ValueError(msg)


def ensure_ms_timestamp(ts: float) -> float:
    """
    Ensure timestamp is in milliseconds.

    That's not 100% bulletproof precise method, because, as usual,
    everything related to time is hard and platform-dependent.

    For example,
      - https://stackoverflow.com/a/50860754
      - https://stackoverflow.com/a/32155104

    :param ts:              Timestamp in seconds or milliseconds.
    :returns:               Corresponding timestamp in milliseconds.
    """
    ts_str_len_s = 10  # 155957458
    current_ts_len = len(str(int(ts)))
    if current_ts_len <= ts_str_len_s:
        converted_ts = round(ts * 1000)
        logger.warning(
            f"Timestamp {ts} appears to be in seconds. Converting to milliseconds: {converted_ts:.0f}.",
        )
        return converted_ts
    return ts
