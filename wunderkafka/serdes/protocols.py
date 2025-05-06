from typing import Final

from wunderkafka.serdes.vendors import get_protocol, get_protocols

MIN_HEADER_SIZE: Final[int] = min([pr.header_size for pr in get_protocols()]) + 1


__all__ = [
    "MIN_HEADER_SIZE",
    "get_protocol",
]
