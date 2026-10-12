import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Verdict(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    VERDICT_UNSPECIFIED: _ClassVar[Verdict]
    VERDICT_CONTINUE: _ClassVar[Verdict]
    VERDICT_PROMOTE: _ClassVar[Verdict]
    VERDICT_ABORT: _ClassVar[Verdict]
VERDICT_UNSPECIFIED: Verdict
VERDICT_CONTINUE: Verdict
VERDICT_PROMOTE: Verdict
VERDICT_ABORT: Verdict

class ListRolloutsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListRolloutsResponse(_message.Message):
    __slots__ = ("rollouts",)
    ROLLOUTS_FIELD_NUMBER: _ClassVar[int]
    rollouts: _containers.RepeatedCompositeFieldContainer[ActiveRollout]
    def __init__(self, rollouts: _Optional[_Iterable[_Union[ActiveRollout, _Mapping]]] = ...) -> None: ...

class ActiveRollout(_message.Message):
    __slots__ = ("release_id", "object", "scope", "version_id", "tenant", "percent", "started_at")
    RELEASE_ID_FIELD_NUMBER: _ClassVar[int]
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    PERCENT_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    release_id: str
    object: str
    scope: str
    version_id: str
    tenant: str
    percent: int
    started_at: _timestamp_pb2.Timestamp
    def __init__(self, release_id: _Optional[str] = ..., object: _Optional[str] = ..., scope: _Optional[str] = ..., version_id: _Optional[str] = ..., tenant: _Optional[str] = ..., percent: _Optional[int] = ..., started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ReportObservationRequest(_message.Message):
    __slots__ = ("release_id", "version_id", "verdict", "summary")
    RELEASE_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    VERDICT_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    release_id: str
    version_id: str
    verdict: Verdict
    summary: str
    def __init__(self, release_id: _Optional[str] = ..., version_id: _Optional[str] = ..., verdict: _Optional[_Union[Verdict, str]] = ..., summary: _Optional[str] = ...) -> None: ...

class ReportObservationResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
