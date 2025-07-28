from __future__ import annotations

import time
from typing import Union, Generic, TypeVar, Optional

from pydantic import Field, BaseModel, ConfigDict, model_validator
from confluent_kafka import Message

from wunderkafka.message import MessageProtocol

T = TypeVar("T")


class PayloadError(BaseModel):
    description: str


class StreamResult(BaseModel, Generic[T]):
    # it's needed to allow attaching the real cimpl.Message in runtime anyway
    model_config = ConfigDict(arbitrary_types_allowed=True)

    payload: Optional[T] = None
    error: Optional[PayloadError] = None
    # not via `|`, because pydantic raises TypeError in python 3.9 without `eval_type_backport` package despite
    # `from __future__ import annotations`
    msg: Union[Message, MessageProtocol]
    t0: float = Field(default_factory=time.perf_counter)

    @property
    def ok(self) -> bool:
        return self.error is None and self.payload is not None

    @property
    def lifetime(self) -> float:
        return time.perf_counter() - self.t0

    @model_validator(mode="after")
    def verify_mutually_exclusive(self) -> StreamResult:
        # The payload may be None due to Kafka tombstones:
        # https://docs.confluent.io/kafka/design/log_compaction.html
        # In short, these are messages with a null value, used to indicate that their key has been deleted.
        if self.payload is not None and self.error is not None:
            raise ValueError("payload and error cannot both be not None")
        return self
