import pytest

from wunderkafka.errors import DeserializerException
from wunderkafka.serdes.vendors import (
    Vendor,
    Actions,
    Suffixes,
    VendorRegistry,
    ProtocolDefinition,
    get_vendors,
    get_protocol,
    get_protocols,
    get_subject_suffix,
)
from wunderkafka.serdes.structures import Mask, Protocol


@pytest.fixture
def currently_known_protocols_count() -> int:
    return 4


@pytest.fixture
def currently_known_vendors_count() -> int:
    return 2


# Tests for VendorRegistry class still need the fixtures
@pytest.fixture
def sample_vendors() -> list[Vendor]:
    return [
        Vendor(
            name="test_vendor1",
            suffixes=Suffixes("_key1", "_value1"),
            protocols=[
                ProtocolDefinition(id=10, protocol=Protocol(4, Mask("I"))),
                ProtocolDefinition(id=11, protocol=Protocol(8, Mask("q"))),
            ],
        ),
        Vendor(
            name="test_vendor2",
            suffixes=Suffixes("_key2", "_value2"),
            protocols=[
                ProtocolDefinition(id=20, protocol=Protocol(12, Mask("qi"))),
            ],
        ),
    ]


@pytest.fixture
def registry(sample_vendors: list[Vendor]) -> VendorRegistry:
    return VendorRegistry(sample_vendors)


def test_init_and_register_vendors(registry: VendorRegistry) -> None:
    answer = 2
    assert len(registry.get_vendors()) == answer


def test_init_and_register_protocols(registry: VendorRegistry) -> None:
    answer = 3
    assert len(registry.get_protocols()) == answer


@pytest.fixture
def duplicated_vendor(sample_vendors: list[Vendor]) -> list[Vendor]:
    return [
        Vendor(
            name="test_vendor1",
            suffixes=Suffixes("_key3", "_value3"),
            protocols=[ProtocolDefinition(id=30, protocol=Protocol(4, Mask("I")))],
        ),
        *sample_vendors,
    ]


def test_init_with_duplicate_vendor_name(duplicated_vendor: list[Vendor]) -> None:
    with pytest.raises(ValueError, match="Vendor .+ already registered"):
        VendorRegistry(duplicated_vendor)


@pytest.fixture
def duplicated_vendor_protocol(sample_vendors: list[Vendor]) -> list[Vendor]:
    return [
        Vendor(
            name="test_vendor3",
            suffixes=Suffixes("_key3", "_value3"),
            protocols=[ProtocolDefinition(id=10, protocol=Protocol(4, Mask("I")))],
        ),
        *sample_vendors,
    ]


def test_init_with_duplicate_protocol_id(duplicated_vendor_protocol: list[Vendor]) -> None:
    with pytest.raises(ValueError, match="Protocol ID 10 already registered"):
        VendorRegistry(duplicated_vendor_protocol)


def test_get_protocol(registry: VendorRegistry) -> None:
    protocol = registry.get_protocol(10)
    assert isinstance(protocol, Protocol)


def test_get_missing_protocol(registry: VendorRegistry) -> None:
    with pytest.raises(KeyError):
        registry.get_protocol(999)


@pytest.fixture
def suffixes(registry: VendorRegistry) -> Suffixes:
    return registry.get_subject_suffixes(10)


def test_get_subject_suffixes_key(suffixes: Suffixes) -> None:
    assert suffixes.key == "_key1"


def test_get_subject_suffixes_value(suffixes: Suffixes) -> None:
    assert suffixes.value == "_value1"


def test_get_missing_subject_suffixes(registry: VendorRegistry) -> None:
    with pytest.raises(KeyError):
        registry.get_subject_suffixes(999)


@pytest.mark.parametrize("protocol_id", [0, 1, 2, 3])
def test_get_protocol_success(protocol_id: int) -> None:
    protocol = get_protocol(protocol_id, Actions.serialize)
    assert isinstance(protocol, Protocol)


def test_get_protocol_error_deserialize() -> None:
    with pytest.raises(DeserializerException, match="Unknown protocol extracted from message"):
        get_protocol(999, Actions.deserialize)


def test_get_protocol_error_serialize() -> None:
    with pytest.raises(ValueError, match="Unknown protocol_id"):
        get_protocol(999, Actions.serialize)


def test_get_subject_suffix_key_confluent() -> None:
    # Test with confluent protocol (ID=0)
    suffix = get_subject_suffix(0, is_key=True)
    assert suffix == "_key"


def test_get_subject_suffix_value_confluent() -> None:
    suffix = get_subject_suffix(0, is_key=False)
    assert suffix == "_value"


def test_get_subject_suffix_key_cloudera() -> None:
    # Test with cloudera protocol (ID=1)
    suffix = get_subject_suffix(1, is_key=True)
    assert suffix == ":k"


def test_get_subject_suffix_value_cloudera() -> None:
    suffix = get_subject_suffix(1, is_key=False)
    assert suffix == ""


def test_get_subject_suffix_unknown_protocol() -> None:
    with pytest.raises(DeserializerException, match="Unknown protocol extracted"):
        get_subject_suffix(999, is_key=True)


def test_get_protocols_function(currently_known_protocols_count: int) -> None:
    protocols = get_protocols()
    assert len(protocols) == currently_known_protocols_count
    assert all(isinstance(p, Protocol) for p in protocols)


@pytest.fixture
def vendors(currently_known_vendors_count: int) -> dict[str, Vendor]:
    vndrs = get_vendors()
    assert len(vndrs) == currently_known_vendors_count
    return {vendor.name: vendor for vendor in vndrs}


@pytest.mark.parametrize("vendor_name", ["confluent", "cloudera"])
def test_get_vendors_function(vendor_name: str, vendors: dict[str, Vendor]) -> None:
    assert vendor_name in vendors


@pytest.fixture
def confluent(vendors: dict[str, Vendor]) -> Vendor:
    return vendors["confluent"]


def test_confluent_vendor_subject_suffix_key(confluent: Vendor) -> None:
    assert confluent.suffixes.key == "_key"


def test_confluent_vendor_subject_suffix_value(confluent: Vendor) -> None:
    assert confluent.suffixes.value == "_value"


@pytest.fixture
def confluent_protocols(confluent: Vendor) -> dict[int, Protocol]:
    protocols = {pr.id: pr.protocol for pr in confluent.protocols}
    assert len(protocols) == 1
    return protocols


@pytest.mark.parametrize("protocol_id", [0])
def test_confluent_vendor_protocol(protocol_id: int, confluent_protocols: dict[int, Protocol]) -> None:
    assert protocol_id in confluent_protocols


@pytest.fixture
def cloudera(vendors: dict[str, Vendor]) -> Vendor:
    return vendors["cloudera"]


def test_cloudera_vendor_subject_suffix_key(cloudera: Vendor) -> None:
    assert cloudera.suffixes.key == ":k"


def test_cloudera_vendor_subject_suffix_value(cloudera: Vendor) -> None:
    assert cloudera.suffixes.value == ""


@pytest.fixture
def cloudera_protocols(cloudera: Vendor) -> dict[int, Protocol]:
    known_cloudera_protocols = 3
    protocols = {pr.id: pr.protocol for pr in cloudera.protocols}
    assert len(protocols) == known_cloudera_protocols
    return protocols


@pytest.mark.parametrize("protocol_id", [1, 2, 3])
def test_cloudera_vendor_protocol(protocol_id: int, cloudera_protocols: dict[int, Protocol]) -> None:
    assert protocol_id in cloudera_protocols
