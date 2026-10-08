import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ExchangeRequest(_message.Message):
    __slots__ = ("delegation", "batch_id")
    DELEGATION_FIELD_NUMBER: _ClassVar[int]
    BATCH_ID_FIELD_NUMBER: _ClassVar[int]
    delegation: str
    batch_id: str
    def __init__(self, delegation: _Optional[str] = ..., batch_id: _Optional[str] = ...) -> None: ...

class ExchangeResponse(_message.Message):
    __slots__ = ("principal", "expires_at")
    PRINCIPAL_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    principal: str
    expires_at: _timestamp_pb2.Timestamp
    def __init__(self, principal: _Optional[str] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
