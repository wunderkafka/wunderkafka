from __future__ import annotations

from enum import Enum
from typing import Final, NamedTuple

from wunderkafka.errors import DeserializerException
from wunderkafka.serdes.structures import Mask, Protocol


class Suffixes(NamedTuple):
    """A pair of suffix strings to append to a Schema Registry's subject for a key and for value."""

    key: str
    value: str


class ProtocolDefinition(NamedTuple):
    """Associates a numeric protocol ID with its Protocol implementation."""

    id: int
    protocol: Protocol


class Vendor(NamedTuple):
    """Holds vendor-specific information for message headers and schema registry."""

    name: str
    suffixes: Suffixes
    protocols: list[ProtocolDefinition]


class VendorRegistry:
    def __init__(self, vendors: list[Vendor]) -> None:
        self.__protocol_id_to_protocol: dict[int, Protocol] = {}
        self.__protocol_id_to_vendor: dict[int, Vendor] = {}

        self.__registered: dict[str, Vendor] = {}
        for vendor in vendors:
            self._register(vendor)

    def get_protocol(self, protocol_id: int) -> Protocol:
        return self.__protocol_id_to_protocol[protocol_id]

    def get_subject_suffixes(self, protocol_id: int) -> Suffixes:
        return self.__protocol_id_to_vendor[protocol_id].suffixes

    def get_protocols(self) -> list[Protocol]:
        return list(self.__protocol_id_to_protocol.values())

    def get_vendors(self) -> list[Vendor]:
        return list(self.__registered.values())

    def _register(self, vendor: Vendor) -> None:
        if vendor.name in self.__registered:
            msg = f"Vendor {vendor!r} already registered"
            raise ValueError(msg)
        for pr in vendor.protocols:
            if pr.id in self.__protocol_id_to_protocol or pr.id in self.__protocol_id_to_vendor:
                msg = f"Protocol ID {pr.id} already registered"
                raise ValueError(msg)
            self.__protocol_id_to_protocol[pr.id] = pr.protocol
            self.__protocol_id_to_vendor[pr.id] = vendor
        self.__registered[vendor.name] = vendor


# see: https://git.io/JvYyC
__VENDOR_REGISTRY: Final[VendorRegistry] = VendorRegistry(
    [
        Vendor(
            name="confluent",
            suffixes=Suffixes("_key", "_value"),
            protocols=[
                # public static final byte CONFLUENT_VERSION_PROTOCOL = 0x0;
                ProtocolDefinition(id=0, protocol=Protocol(4, Mask("I"))),
            ],
        ),
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
            ],
        ),
    ]
)


class Actions(str, Enum):
    deserialize = "deserialize"
    serialize = "serialize"


def get_protocol(protocol_id: int, action: Actions) -> Protocol:
    """Retrieve the Protocol implementation for this ID, wrapping errors by action."""
    try:
        return __VENDOR_REGISTRY.get_protocol(protocol_id)
    except KeyError as exc:
        if action == Actions.deserialize:
            msg = f"Unknown protocol extracted from message: {protocol_id}"
            raise DeserializerException(msg) from exc
        msg = f"Unknown protocol_id {protocol_id}!"
        raise ValueError(msg) from exc


def get_subject_suffix(protocol_id: int, *, is_key: bool) -> str:
    """Fetch the correct subject suffix a Schema Registry's subject (key/value)."""
    try:
        suffixes = __VENDOR_REGISTRY.get_subject_suffixes(protocol_id)
    except KeyError as exc:
        # Strictly speaking, if we get unknown protocol_id here,
        # we should raise an exception much earlier (during header unpacking step short after the message was received).
        # But as a Schema Registry client is a separate moving part, we decided to add fuse here
        msg = f"Unknown protocol extracted from message: {protocol_id}"
        raise DeserializerException(msg) from exc
    else:
        return suffixes.key if is_key else suffixes.value


def get_protocols() -> list[Protocol]:
    """Returns a list of all registered header protocols."""
    return __VENDOR_REGISTRY.get_protocols()


def get_vendors() -> list[Vendor]:
    """Returns a list of all registered vendors."""
    return __VENDOR_REGISTRY.get_vendors()
