"""This module contains subscription helpers."""

from __future__ import annotations

import datetime

from confluent_kafka import OFFSET_END, OFFSET_BEGINNING

from wunderkafka.time import ensure_ms_timestamp
from wunderkafka.structures import Offset, Timestamp


def choose_offset(
    *,
    from_beginning: bool | None = None,
    offset: int | None = None,
    ts: float | None = None,
    dt: datetime.datetime | None = None,
    with_timedelta: datetime.timedelta | None = None,
) -> Offset | Timestamp | None:
    """
    Choose subscription method (Offset, Timestamp, None), corresponding to given values.

    :param from_beginning:  If a flag is set, return specific offset corresponding to the beginning /end of the topic.
    :param offset:          If set, will return an Offset object.
    :param ts:              If set, will return Timestamp.
    :param dt:              If set, will get Timestamp from a datetime object.
    :param with_timedelta:  If set, will calculate timestamp for corresponding timedelta.

    :raises ValueError:     If multiple values are set: there is no sense in attempt to subscribe to a topic via
                            multiple offsets (excluding a multiple partitions scenario, which is not currently
                            supported).

    :return:                None if no values are specified, Offset or Timestamp otherwise.
    """
    args = (from_beginning, offset, ts, with_timedelta)
    if sum([arg is not None for arg in args]) > 1:
        msg = "Only one subscription method per time is allowed!"
        raise ValueError(msg)
    if from_beginning is not None:
        offset = OFFSET_BEGINNING if from_beginning else OFFSET_END
    if offset is not None:
        return Offset(offset)
    if with_timedelta is not None:
        start = datetime.datetime.now(datetime.timezone.utc) - with_timedelta
        ts = start.timestamp() * 1000
    if dt is not None:
        ts = dt.timestamp() * 1000
    if ts is not None:
        return Timestamp(int(ensure_ms_timestamp(ts)))
    return None


class TopicSubscription:
    """Allows custom definition of subscription for a topic without the need to build a full TopicPartition list."""

    # # TODO (tribunsky.kir): reconsider API of 'how'
    #                         https://github.com/severstal-digital/wunderkafka/issues/89
    def __init__(  # noqa: PLR0913
        self,
        topic: str,
        *,
        from_beginning: bool | None = None,
        offset: int | None = None,
        ts: float | None = None,
        dt: datetime.datetime | None = None,
        with_timedelta: datetime.timedelta | None = None,
    ) -> None:
        """
        Init topic subscription object.

        Only one method of subscription per topic is allowed at a time:

        - From the beginning (depends on your retention policy)
        - from the end (consume only latest messages)
        - via specific offset
        - via specific timestamp
        - via specific datetime (datetime.datetime object)
        - via specific timedelta (from current datetime)
        - no special option (consumer will use "default" value of auto.offset.reset)

        :param topic:           Topic to subscribe.
        :param from_beginning:  If True, subscribe to get the earliest available messages.
                                If False, get the latest messages.
        :param offset:          Subscribe to specific offset.
                                If offset not found, it will behave with built-in default.
        :param ts:              Subscribe to specific timestamp (milliseconds).
                                If timestamp is not found, will behave with built-in default.
        :param dt:              Subscribe to specific datetime.
                                If datetime is not found, will behave with built-in default.
        :param with_timedelta:  Subscribe to some moments in the past, from current datetime for a given timedelta.
                                Will calculate specific timestamp and subscribe via ts.
        """
        self.topic = topic
        self.how = choose_offset(
            from_beginning=from_beginning,
            offset=offset,
            ts=ts,
            dt=dt,
            with_timedelta=with_timedelta,
        )
