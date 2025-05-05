from __future__ import annotations

from enum import Enum
from typing import NamedTuple

from wunderkafka.errors import DeserializerException
from wunderkafka.serdes.structures import Mask, Protocol


class Suffixes(NamedTuple):
    key: str
    value: str


class ProtocolDefinition(NamedTuple):
    id: int
    protocol: Protocol


class Vendor(NamedTuple):
    name: str
    suffixes: Suffixes
    protocols: list[ProtocolDefinition]


class VendorRegistry:
    def __init__(self, vendors: list[Vendor]) -> None:
        self._protocol_id_to_protocol: dict[int, Protocol] = {}
        self._protocol_id_to_suffixes: dict[int, Suffixes] = {}
        for vendor in vendors:
            self._register(vendor)

    def _register(self, vendor: Vendor) -> None:
        for pr in vendor.protocols:
            self._protocol_id_to_protocol[pr.id] = pr.protocol
            self._protocol_id_to_suffixes[pr.id] = vendor.suffixes

    def get_protocol(self, protocol_id: int) -> Protocol | None:
        return self._protocol_id_to_protocol.get(protocol_id)

    def get_subject_suffixes(self, protocol_id: int) -> Suffixes | None:
        return self._protocol_id_to_suffixes.get(protocol_id)

    def get_protocols(self) -> list[Protocol]:
        return list(self._protocol_id_to_protocol.values())

# see: https://git.io/JvYyC
_VENDOR_REGISTRY = VendorRegistry([
    Vendor(
        name="confluent",
        suffixes=Suffixes("_key", "_value"),
        protocols=[
            # public static final byte CONFLUENT_VERSION_PROTOCOL = 0x0;
            ProtocolDefinition(id=0, protocol=Protocol(4, Mask("I"))),
        ]),
    Vendor(
        name="cloudera",
        suffixes=Suffixes(":k", ""),
        protocols=[
            # public static final byte METADATA_ID_VERSION_PROTOCOL = 0x1;
            ProtocolDefinition(id=1, protocol=Protocol(12, Mask("qi"))),
            # public static final byte VERSION_ID_AS_LONG_PROTOCOL = 0x2;
            ProtocolDefinition(id=2, protocol=Protocol(8, Mask("q"))),
            # public static final byte VERSION_ID_AS_INT_PROTOCOL = 0x3;
            ProtocolDefinition(id=3, protocol=Protocol(4, Mask("I"))),
        ]),
])

class Actions(str, Enum):
    deserialize = "deserialize"
    serialize = "serialize"


def get_protocol(protocol_id: int, action: Actions) -> Protocol:
    protocol = _VENDOR_REGISTRY.get_protocol(protocol_id)
    if protocol is not None:
        return protocol
    if action == Actions.deserialize:
        msg = f"Unknown protocol extracted from message: {protocol_id}"
        raise DeserializerException(msg)
    msg = f"Unknown protocol_id {protocol_id}!"
    raise ValueError(msg)


def get_subject_suffix(protocol_id: int, *, is_key: bool) -> str:
    suffixes = _VENDOR_REGISTRY.get_subject_suffixes(protocol_id)
    if suffixes is not None:
        return suffixes.key if is_key else suffixes.value

    # Strictly speaking, if we get unknown protocol_id here,
    # we should raise an exception much earlier (during header unpacking step short after the message was received).
    # But as a Schema Registry client is a separate moving part, we decided to add fuse here
    msg = f"Unknown protocol extracted from message: {protocol_id}"
    raise DeserializerException(msg)

def get_protocols() -> list[Protocol]:
    return _VENDOR_REGISTRY.get_protocols()
