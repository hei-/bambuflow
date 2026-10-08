import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DenialScope(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DENIAL_SCOPE_UNSPECIFIED: _ClassVar[DenialScope]
    DENIAL_SCOPE_SOURCE: _ClassVar[DenialScope]
    DENIAL_SCOPE_DOCUMENT: _ClassVar[DenialScope]
    DENIAL_SCOPE_TENANT: _ClassVar[DenialScope]

class DenialStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DENIAL_STATUS_UNSPECIFIED: _ClassVar[DenialStatus]
    DENIAL_STATUS_PENDING: _ClassVar[DenialStatus]
    DENIAL_STATUS_ACTIVE: _ClassVar[DenialStatus]
DENIAL_SCOPE_UNSPECIFIED: DenialScope
DENIAL_SCOPE_SOURCE: DenialScope
DENIAL_SCOPE_DOCUMENT: DenialScope
DENIAL_SCOPE_TENANT: DenialScope
DENIAL_STATUS_UNSPECIFIED: DenialStatus
DENIAL_STATUS_PENDING: DenialStatus
DENIAL_STATUS_ACTIVE: DenialStatus

class EmergencyDenial(_message.Message):
    __slots__ = ("denial_id", "scope", "target", "reason", "created_by", "created_at", "status", "applied_at")
    DENIAL_ID_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    APPLIED_AT_FIELD_NUMBER: _ClassVar[int]
    denial_id: str
    scope: DenialScope
    target: str
    reason: str
    created_by: str
    created_at: _timestamp_pb2.Timestamp
    status: DenialStatus
    applied_at: _timestamp_pb2.Timestamp
    def __init__(self, denial_id: _Optional[str] = ..., scope: _Optional[_Union[DenialScope, str]] = ..., target: _Optional[str] = ..., reason: _Optional[str] = ..., created_by: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., status: _Optional[_Union[DenialStatus, str]] = ..., applied_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class CreateEmergencyDenialRequest(_message.Message):
    __slots__ = ("scope", "target", "reason")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    scope: DenialScope
    target: str
    reason: str
    def __init__(self, scope: _Optional[_Union[DenialScope, str]] = ..., target: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class CreateEmergencyDenialResponse(_message.Message):
    __slots__ = ("denial", "created")
    DENIAL_FIELD_NUMBER: _ClassVar[int]
    CREATED_FIELD_NUMBER: _ClassVar[int]
    denial: EmergencyDenial
    created: bool
    def __init__(self, denial: _Optional[_Union[EmergencyDenial, _Mapping]] = ..., created: _Optional[bool] = ...) -> None: ...

class ListEmergencyDenialsRequest(_message.Message):
    __slots__ = ("page_size", "page_token")
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    page_size: int
    page_token: str
    def __init__(self, page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListEmergencyDenialsResponse(_message.Message):
    __slots__ = ("denials", "next_page_token")
    DENIALS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    denials: _containers.RepeatedCompositeFieldContainer[EmergencyDenial]
    next_page_token: str
    def __init__(self, denials: _Optional[_Iterable[_Union[EmergencyDenial, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...
