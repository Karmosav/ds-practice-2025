from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class GetStatusRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetStatusResponse(_message.Message):
    __slots__ = ("executor_id", "is_leader", "leader_id", "processed_orders")
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    IS_LEADER_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    PROCESSED_ORDERS_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    is_leader: bool
    leader_id: str
    processed_orders: int
    def __init__(self, executor_id: _Optional[str] = ..., is_leader: bool = ..., leader_id: _Optional[str] = ..., processed_orders: _Optional[int] = ...) -> None: ...
