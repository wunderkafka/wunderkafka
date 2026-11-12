import sys

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


if sys.version_info >= (3, 10):
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec


if sys.version_info >= (3, 15):
    from typing import TypeForm
else:
    from typing_extensions import TypeForm


__all__ = [
    "ParamSpec",
    "Self",
    "TypeForm",
]
