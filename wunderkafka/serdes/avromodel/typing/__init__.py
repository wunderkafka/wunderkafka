from typing import TYPE_CHECKING, Any, Optional, get_args, get_origin

if TYPE_CHECKING:
    from typing_extensions import TypeForm


def is_generic_type(annotation: "Optional[TypeForm[Any]]") -> bool:
    return get_origin(annotation) is not None and len(get_args(annotation)) > 0
