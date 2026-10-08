import datetime

from bambuflow.common.v1 import types_pb2 as _types_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SourceStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SOURCE_STATUS_UNSPECIFIED: _ClassVar[SourceStatus]
    SOURCE_STATUS_PENDING: _ClassVar[SourceStatus]
    SOURCE_STATUS_ACTIVE: _ClassVar[SourceStatus]
    SOURCE_STATUS_REVOKED: _ClassVar[SourceStatus]
SOURCE_STATUS_UNSPECIFIED: SourceStatus
SOURCE_STATUS_PENDING: SourceStatus
SOURCE_STATUS_ACTIVE: SourceStatus
SOURCE_STATUS_REVOKED: SourceStatus

class Source(_message.Message):
    __slots__ = ("source_id", "type", "owner_dept", "default_classification", "description", "status", "created_by", "created_at", "approved_by", "approved_at", "approval_basis", "revoked_by", "revoked_at", "revoke_reason")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    OWNER_DEPT_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    APPROVED_BY_FIELD_NUMBER: _ClassVar[int]
    APPROVED_AT_FIELD_NUMBER: _ClassVar[int]
    APPROVAL_BASIS_FIELD_NUMBER: _ClassVar[int]
    REVOKED_BY_FIELD_NUMBER: _ClassVar[int]
    REVOKED_AT_FIELD_NUMBER: _ClassVar[int]
    REVOKE_REASON_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    type: _types_pb2.SourceType
    owner_dept: str
    default_classification: _types_pb2.Classification
    description: str
    status: SourceStatus
    created_by: str
    created_at: _timestamp_pb2.Timestamp
    approved_by: str
    approved_at: _timestamp_pb2.Timestamp
    approval_basis: str
    revoked_by: str
    revoked_at: _timestamp_pb2.Timestamp
    revoke_reason: str
    def __init__(self, source_id: _Optional[str] = ..., type: _Optional[_Union[_types_pb2.SourceType, str]] = ..., owner_dept: _Optional[str] = ..., default_classification: _Optional[_Union[_types_pb2.Classification, str]] = ..., description: _Optional[str] = ..., status: _Optional[_Union[SourceStatus, str]] = ..., created_by: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., approved_by: _Optional[str] = ..., approved_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., approval_basis: _Optional[str] = ..., revoked_by: _Optional[str] = ..., revoked_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., revoke_reason: _Optional[str] = ...) -> None: ...

class RegisterSourceRequest(_message.Message):
    __slots__ = ("type", "owner_dept", "default_classification", "description")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    OWNER_DEPT_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    type: _types_pb2.SourceType
    owner_dept: str
    default_classification: _types_pb2.Classification
    description: str
    def __init__(self, type: _Optional[_Union[_types_pb2.SourceType, str]] = ..., owner_dept: _Optional[str] = ..., default_classification: _Optional[_Union[_types_pb2.Classification, str]] = ..., description: _Optional[str] = ...) -> None: ...

class RegisterSourceResponse(_message.Message):
    __slots__ = ("source",)
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    source: Source
    def __init__(self, source: _Optional[_Union[Source, _Mapping]] = ...) -> None: ...

class ApproveSourceRequest(_message.Message):
    __slots__ = ("source_id", "basis")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    BASIS_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    basis: str
    def __init__(self, source_id: _Optional[str] = ..., basis: _Optional[str] = ...) -> None: ...

class ApproveSourceResponse(_message.Message):
    __slots__ = ("source",)
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    source: Source
    def __init__(self, source: _Optional[_Union[Source, _Mapping]] = ...) -> None: ...

class RevokeSourceRequest(_message.Message):
    __slots__ = ("source_id", "reason")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    reason: str
    def __init__(self, source_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class RevokeSourceResponse(_message.Message):
    __slots__ = ("source", "denial_id")
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    DENIAL_ID_FIELD_NUMBER: _ClassVar[int]
    source: Source
    denial_id: str
    def __init__(self, source: _Optional[_Union[Source, _Mapping]] = ..., denial_id: _Optional[str] = ...) -> None: ...

class Grant(_message.Message):
    __slots__ = ("source_id", "tenant", "granted_by", "granted_at")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    GRANTED_BY_FIELD_NUMBER: _ClassVar[int]
    GRANTED_AT_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    tenant: str
    granted_by: str
    granted_at: _timestamp_pb2.Timestamp
    def __init__(self, source_id: _Optional[str] = ..., tenant: _Optional[str] = ..., granted_by: _Optional[str] = ..., granted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ListGrantsRequest(_message.Message):
    __slots__ = ("source_id",)
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    def __init__(self, source_id: _Optional[str] = ...) -> None: ...

class ListGrantsResponse(_message.Message):
    __slots__ = ("grants",)
    GRANTS_FIELD_NUMBER: _ClassVar[int]
    grants: _containers.RepeatedCompositeFieldContainer[Grant]
    def __init__(self, grants: _Optional[_Iterable[_Union[Grant, _Mapping]]] = ...) -> None: ...

class CreateGrantRequest(_message.Message):
    __slots__ = ("source_id", "tenant")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    tenant: str
    def __init__(self, source_id: _Optional[str] = ..., tenant: _Optional[str] = ...) -> None: ...

class CreateGrantResponse(_message.Message):
    __slots__ = ("grant", "created")
    GRANT_FIELD_NUMBER: _ClassVar[int]
    CREATED_FIELD_NUMBER: _ClassVar[int]
    grant: Grant
    created: bool
    def __init__(self, grant: _Optional[_Union[Grant, _Mapping]] = ..., created: _Optional[bool] = ...) -> None: ...

class DeleteGrantRequest(_message.Message):
    __slots__ = ("source_id", "tenant")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    tenant: str
    def __init__(self, source_id: _Optional[str] = ..., tenant: _Optional[str] = ...) -> None: ...

class DeleteGrantResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
