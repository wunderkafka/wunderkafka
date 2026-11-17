import inspect
from types import UnionType
from typing import Any, Annotated, get_origin


def create_annotation(generic: Any, types_list: list[type[object]]) -> type[object]:
    # return generic[Union[types_list]]
    return generic[tuple(types_list)]


def is_union_type(generic: Any) -> bool:
    return inspect.isclass(generic) and issubclass(generic, UnionType)


def get_generic(annotation: Any) -> Any:
    return get_origin(annotation)


def is_annotated_type(annotation: Any) -> bool:
    return get_origin(annotation) is Annotated
