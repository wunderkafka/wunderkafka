import struct

from wunderkafka.errors import DeserializerException
from wunderkafka.serdes.abc import AbstractProtocolHandler
from wunderkafka.structures import SRMeta, ParsedHeader
from wunderkafka.serdes.vendors import Actions
from wunderkafka.serdes.protocols import MIN_HEADER_SIZE, get_protocol

# TODO (tribunsky.kir): Get rid of AbstractProtocolHandler?
#                       https://github.com/


class ConfluentClouderaHeadersHandler(AbstractProtocolHandler):
    def parse(self, blob: bytes) -> ParsedHeader:
        if len(blob) < MIN_HEADER_SIZE:
            msg = f"Message is too small to decode: ({blob!r})"
            raise DeserializerException(msg)

        # 1st byte is magic byte.
        [protocol_id] = struct.unpack(">b", blob[0:1])

        protocol = get_protocol(protocol_id, Actions.deserialize)

        # already read 1 byte from header as protocol id
        meta = struct.unpack(protocol.mask.unpack, blob[1 : 1 + protocol.header_size])

        if protocol_id == 1:
            schema_id = None
            schema_meta_id, schema_version = meta
        else:
            [schema_id] = meta
            schema_meta_id = None
            schema_version = None

        return ParsedHeader(
            protocol_id=protocol_id,
            meta_id=schema_meta_id,
            schema_id=schema_id,
            schema_version=schema_version,
            size=protocol.header_size + 1,
        )

    def pack(self, protocol_id: int, meta: SRMeta) -> bytes:
        protocol = get_protocol(protocol_id, Actions.serialize)

        if protocol_id == 1:
            if meta.meta_id is None:
                err_msg = f"No meta id for protocol_id {protocol_id}. Please, check response from Schema Registry."
                raise ValueError(err_msg)
            return struct.pack(protocol.mask.pack, protocol_id, meta.meta_id, meta.schema_version)
        return struct.pack(protocol.mask.pack, protocol_id, meta.schema_id)
