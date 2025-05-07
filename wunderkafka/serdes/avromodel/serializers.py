from typing import Any, Optional
from dataclasses import asdict, is_dataclass

from pydantic import BaseModel

from wunderkafka.compat import ParamSpec
from wunderkafka.serdes.abc import AbstractSerializer, AbstractDescriptionStore
from wunderkafka.serdes.avro import FastAvroSerializer

P = ParamSpec("P")


class AvroModelSerializer(AbstractSerializer):
    def __init__(self, store: Optional[AbstractDescriptionStore] = None) -> None:
        self._serializer = FastAvroSerializer()
        self.store = store

    def serialize(
        self,
        schema: str,
        payload: Any,
        header: Optional[bytes] = None,
        *args: P.args,  # type: ignore[valid-type]
        **kwargs: P.kwargs,  # type: ignore[valid-type]
    ) -> bytes:
        if isinstance(payload, BaseModel):
            dct = payload.model_dump()
        else:
            dct = asdict(payload) if is_dataclass(payload) else dict(payload)  # type: ignore[arg-type]
        return self._serializer.serialize(schema, dct, header)
