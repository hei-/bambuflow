import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PlatformEvent(_message.Message):
    __slots__ = ("event_id", "type", "tenant", "object_id", "occurred_at")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    OBJECT_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    type: str
    tenant: str
    object_id: str
    occurred_at: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., type: _Optional[str] = ..., tenant: _Optional[str] = ..., object_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
