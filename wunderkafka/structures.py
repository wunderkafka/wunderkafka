"""This module contains common datastructures which is used in the package."""

from __future__ import annotations

import datetime
from enum import Enum
from dataclasses import dataclass

from wunderkafka.time import ts2dt
from wunderkafka.serdes.vendors import get_subject_suffix


class SchemaType(str, Enum):
    AVRO = "AVRO"
    JSON = "JSON"
    PROTOBUF = "PROTOBUF"
    # Strings, doubles, integers
    PRIMITIVES = "PRIMITIVES"


@dataclass(frozen=True)
class Timestamp:
    value: int

    def __str__(self) -> str:
        tz_local = datetime.datetime.now().astimezone().tzinfo
        return f"{self.__class__.__name__}: {self.value:.2f} ({ts2dt(self.value).astimezone(tz_local)})"


@dataclass(frozen=True)
class Offset:
    value: int


# TODO (tribunsky.kir): compose header & meta in symmetrical way
#                       https://github.com/severstal-digital/wunderkafka/issues/88
@dataclass(frozen=True)
class ParsedHeader:
    protocol_id: int
    meta_id: int | None
    schema_id: int | None
    schema_version: int | None
    size: int


@dataclass(frozen=True)
class SchemaMeta:
    topic: str
    is_key: bool
    header: ParsedHeader

    @property
    def subject(self) -> str:
        """
        Return topic mapped to schema registry entity.

        :return:            String which should be used as a path in schema registry's url
                            corresponding to a given topic.
        """
        suffix = get_subject_suffix(self.header.protocol_id, is_key=self.is_key)
        return f"{self.topic}{suffix}"


# TODO (tribunsky.kir): add cross-validation of invariants on the model itself?
#                       This awkward class may be gone with:
#                       https://github.com/severstal-digital/wunderkafka/issues/88
@dataclass(frozen=True)
class SRMeta:
    """Meta, which is retrieved after schema registration."""

    # Confluent always has schema_id, but one of cloudera protocols doesn't use it
    schema_id: int | None
    # Confluent has schema_version, but serdes works around schema_id, which is a unique identifier there.
    schema_version: int | None
    # Confluent doesn't have metaId
    meta_id: int | None = None


@dataclass(frozen=True)
class SerializerSchemaDescription:
    """
    Class to allow a contract extension between moving parts of serialization.

    Usually schema is represented by text, but separate class adds abstraction.
    """

    text: str

    @property
    def empty(self) -> bool:
        """
        Return True if schema is empty.

        Some serializers (e.g., confluent StringSerializer) practically don't have an effective schema.
        """
        return self.text == ""


@dataclass(frozen=True)
class DeserializerSchemaDescription(SerializerSchemaDescription):
    """
    Class to allow a contract extension between moving parts of deserialization.

    Usually schema is represented by text, but separate class adds abstraction.
    """

    text: str
    type: SchemaType
