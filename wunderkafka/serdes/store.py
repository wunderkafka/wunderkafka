from typing import Union, Optional
from pathlib import Path

from pydantic import BaseModel
from pydantic.json_schema import GenerateJsonSchema
from dataclasses_avroschema import AvroModel

from wunderkafka.types import TopicName, KeySchemaDescription, ValueSchemaDescription
from wunderkafka.serdes import avromodel, jsonmodel
from wunderkafka.serdes.abc import AbstractDescriptionStore
from wunderkafka.structures import SchemaType
from wunderkafka.serdes.jsonmodel.derive import JSONClosedModelGenerator


class SchemaTextRepo(AbstractDescriptionStore):
    def __init__(self, schema_type: SchemaType) -> None:
        super().__init__()
        self.schema_type = schema_type

    def add(self, topic: TopicName, value: str, key: str) -> None:
        self._values[topic] = ValueSchemaDescription(text=value, type=self.schema_type)
        if key is not None:
            self._keys[topic] = KeySchemaDescription(text=key, type=self.schema_type)


def _load_from_file(filename: Path) -> str:
    with open(filename) as fl:
        return fl.read().strip()


# FixMe (tribunsky.kir): for now it looks like a crutch, but much better than just `if StringSerializer`
class DummyRepo(AbstractDescriptionStore):
    def add(self, topic: TopicName, value: Union[str, Path], key: Union[str, Path]) -> None:
        # schema = '{"schema": "{\"type\": \"string\"}"}'
        schema = ""
        self._values[topic] = ValueSchemaDescription(text=schema, type=SchemaType.PRIMITIVES)
        if key is not None:
            self._keys[topic] = KeySchemaDescription(text=schema, type=SchemaType.PRIMITIVES)


class StringRepo(DummyRepo): ...


# ToDo (tribunsky.kir): refactor it, maybe add hooks to parent class.
#                       Barbara, forgive us. Looks like AbstractDescriptionStore should be generic.
class SchemaFSRepo(AbstractDescriptionStore):
    def __init__(self, schema_type: SchemaType) -> None:
        super().__init__()
        self.schema_type = schema_type

    def add(self, topic: TopicName, value: Union[str, Path], key: Union[str, Path]) -> None:
        self._values[topic] = ValueSchemaDescription(text=_load_from_file(Path(value)), type=self.schema_type)
        if key is not None:
            self._keys[topic] = KeySchemaDescription(text=_load_from_file(Path(key)), type=self.schema_type)


class AvroModelRepo(AbstractDescriptionStore):
    # ToDo (tribunsky.kir): change Type[AvroModel] to more general alias + check derivation from python built-ins
    def add(self, topic: TopicName, value: type[AvroModel], key: Optional[type[AvroModel]]) -> None:
        self._values[topic] = ValueSchemaDescription(text=avromodel.derive(value, topic), type=SchemaType.AVRO)
        if key is not None:
            self._keys[topic] = KeySchemaDescription(
                text=avromodel.derive(key, topic, is_key=True),
                type=SchemaType.AVRO,
            )


class JSONRepo(AbstractDescriptionStore):
    def add(self, topic: TopicName, value: str, key: Optional[str]) -> None:
        self._values[topic] = ValueSchemaDescription(text=value, type=SchemaType.JSON)
        if key is not None:
            self._keys[topic] = KeySchemaDescription(text=key, type=SchemaType.JSON)


class JSONModelRepo(AbstractDescriptionStore):
    def __init__(self, schema_generator: type[GenerateJsonSchema] = JSONClosedModelGenerator) -> None:
        super().__init__()
        self._schema_generator = schema_generator

    def add(self, topic: TopicName, value: type[BaseModel], key: Optional[type[BaseModel]]) -> None:
        self._values[topic] = ValueSchemaDescription(
            text=jsonmodel.derive(value, self._schema_generator),
            type=SchemaType.JSON,
        )
        if key is not None:
            self._keys[topic] = KeySchemaDescription(
                text=jsonmodel.derive(key, self._schema_generator),
                type=SchemaType.JSON,
            )
