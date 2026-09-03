from typing import TypeVar, Generic

_RT = TypeVar("_RT", bound=int)

class new_class(Generic[_RT]):
    def __init__(self):
        self.num = 10

    def __call__(self) -> _RT:
        return self.num ** 2

inst = new_class()
val = inst()