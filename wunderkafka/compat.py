import sys
from typing import Self, ParamSpec

if sys.version_info >= (3, 15):
    from typing import TypeForm
else:
    from typing_extensions import TypeForm


__all__ = [
    "ParamSpec",
    "Self",
    "TypeForm",
]
