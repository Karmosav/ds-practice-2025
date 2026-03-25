from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class OrderEnvelope(_message.Message):
    __slots__ = ("order_id", "order_payload_json")
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    ORDER_PAYLOAD_JSON_FIELD_NUMBER: _ClassVar[int]
    order_id: str
    order_payload_json: str
    def __init__(self, order_id: _Optional[str] = ..., order_payload_json: _Optional[str] = ...) -> None: ...

class EnqueueRequest(_message.Message):
    __slots__ = ("order",)
    ORDER_FIELD_NUMBER: _ClassVar[int]
    order: OrderEnvelope
    def __init__(self, order: _Optional[_Union[OrderEnvelope, _Mapping]] = ...) -> None: ...

class EnqueueResponse(_message.Message):
    __slots__ = ("enqueued", "message", "queue_size")
    ENQUEUED_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    QUEUE_SIZE_FIELD_NUMBER: _ClassVar[int]
    enqueued: bool
    message: str
    queue_size: int
    def __init__(self, enqueued: bool = ..., message: _Optional[str] = ..., queue_size: _Optional[int] = ...) -> None: ...

class DequeueRequest(_message.Message):
    __slots__ = ("executor_id",)
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    def __init__(self, executor_id: _Optional[str] = ...) -> None: ...

class DequeueResponse(_message.Message):
    __slots__ = ("has_order", "order", "message")
    HAS_ORDER_FIELD_NUMBER: _ClassVar[int]
    ORDER_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    has_order: bool
    order: OrderEnvelope
    message: str
    def __init__(self, has_order: bool = ..., order: _Optional[_Union[OrderEnvelope, _Mapping]] = ..., message: _Optional[str] = ...) -> None: ...

class RegisterExecutorRequest(_message.Message):
    __slots__ = ("executor_id",)
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    def __init__(self, executor_id: _Optional[str] = ...) -> None: ...

class ExecutorHeartbeatRequest(_message.Message):
    __slots__ = ("executor_id",)
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    def __init__(self, executor_id: _Optional[str] = ...) -> None: ...

class GetLeaderRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class LeaderResponse(_message.Message):
    __slots__ = ("granted", "leader_id", "lease_expires_unix_ms", "message")
    GRANTED_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    LEASE_EXPIRES_UNIX_MS_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    granted: bool
    leader_id: str
    lease_expires_unix_ms: int
    message: str
    def __init__(self, granted: bool = ..., leader_id: _Optional[str] = ..., lease_expires_unix_ms: _Optional[int] = ..., message: _Optional[str] = ...) -> None: ...
