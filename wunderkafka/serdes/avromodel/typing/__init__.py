from typing import Any, Optional, get_args, get_origin

from wunderkafka.compat import TypeForm


def is_generic_type(annotation: Optional[TypeForm[Any]]) -> bool:
    return get_origin(annotation) is not None and len(get_args(annotation)) > 0
