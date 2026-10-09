import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ConfigVersion(_message.Message):
    __slots__ = ("version_id", "object", "scope", "number", "content", "content_hash", "created_by", "created_at", "source_ref", "active")
    VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    NUMBER_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    SOURCE_REF_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    version_id: str
    object: str
    scope: str
    number: int
    content: str
    content_hash: str
    created_by: str
    created_at: _timestamp_pb2.Timestamp
    source_ref: str
    active: bool
    def __init__(self, version_id: _Optional[str] = ..., object: _Optional[str] = ..., scope: _Optional[str] = ..., number: _Optional[int] = ..., content: _Optional[str] = ..., content_hash: _Optional[str] = ..., created_by: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., source_ref: _Optional[str] = ..., active: _Optional[bool] = ...) -> None: ...

class TenantSummary(_message.Message):
    __slots__ = ("tenant", "rollout", "active")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    ROLLOUT_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    rollout: str
    active: ConfigVersion
    def __init__(self, tenant: _Optional[str] = ..., rollout: _Optional[str] = ..., active: _Optional[_Union[ConfigVersion, _Mapping]] = ...) -> None: ...

class ListTenantsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListTenantsResponse(_message.Message):
    __slots__ = ("tenants",)
    TENANTS_FIELD_NUMBER: _ClassVar[int]
    tenants: _containers.RepeatedCompositeFieldContainer[TenantSummary]
    def __init__(self, tenants: _Optional[_Iterable[_Union[TenantSummary, _Mapping]]] = ...) -> None: ...

class ListConfigVersionsRequest(_message.Message):
    __slots__ = ("object", "scope")
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    object: str
    scope: str
    def __init__(self, object: _Optional[str] = ..., scope: _Optional[str] = ...) -> None: ...

class ListConfigVersionsResponse(_message.Message):
    __slots__ = ("versions",)
    VERSIONS_FIELD_NUMBER: _ClassVar[int]
    versions: _containers.RepeatedCompositeFieldContainer[ConfigVersion]
    def __init__(self, versions: _Optional[_Iterable[_Union[ConfigVersion, _Mapping]]] = ...) -> None: ...

class ChangeRequest(_message.Message):
    __slots__ = ("chg_id", "object", "scope", "base_version_id", "version", "reason", "gate_id", "author", "created_at", "status", "decided_by", "decided_at", "reject_reason", "release_id")
    CHG_ID_FIELD_NUMBER: _ClassVar[int]
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    BASE_VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    GATE_ID_FIELD_NUMBER: _ClassVar[int]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    DECIDED_BY_FIELD_NUMBER: _ClassVar[int]
    DECIDED_AT_FIELD_NUMBER: _ClassVar[int]
    REJECT_REASON_FIELD_NUMBER: _ClassVar[int]
    RELEASE_ID_FIELD_NUMBER: _ClassVar[int]
    chg_id: str
    object: str
    scope: str
    base_version_id: str
    version: ConfigVersion
    reason: str
    gate_id: str
    author: str
    created_at: _timestamp_pb2.Timestamp
    status: str
    decided_by: str
    decided_at: _timestamp_pb2.Timestamp
    reject_reason: str
    release_id: str
    def __init__(self, chg_id: _Optional[str] = ..., object: _Optional[str] = ..., scope: _Optional[str] = ..., base_version_id: _Optional[str] = ..., version: _Optional[_Union[ConfigVersion, _Mapping]] = ..., reason: _Optional[str] = ..., gate_id: _Optional[str] = ..., author: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., status: _Optional[str] = ..., decided_by: _Optional[str] = ..., decided_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., reject_reason: _Optional[str] = ..., release_id: _Optional[str] = ...) -> None: ...

class CreateChangeRequestRequest(_message.Message):
    __slots__ = ("object", "scope", "base_version_id", "content", "reason", "gate_id")
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    BASE_VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    GATE_ID_FIELD_NUMBER: _ClassVar[int]
    object: str
    scope: str
    base_version_id: str
    content: str
    reason: str
    gate_id: str
    def __init__(self, object: _Optional[str] = ..., scope: _Optional[str] = ..., base_version_id: _Optional[str] = ..., content: _Optional[str] = ..., reason: _Optional[str] = ..., gate_id: _Optional[str] = ...) -> None: ...

class CreateChangeRequestResponse(_message.Message):
    __slots__ = ("change_request",)
    CHANGE_REQUEST_FIELD_NUMBER: _ClassVar[int]
    change_request: ChangeRequest
    def __init__(self, change_request: _Optional[_Union[ChangeRequest, _Mapping]] = ...) -> None: ...

class ApproveChangeRequestRequest(_message.Message):
    __slots__ = ("chg_id",)
    CHG_ID_FIELD_NUMBER: _ClassVar[int]
    chg_id: str
    def __init__(self, chg_id: _Optional[str] = ...) -> None: ...

class ApproveChangeRequestResponse(_message.Message):
    __slots__ = ("change_request",)
    CHANGE_REQUEST_FIELD_NUMBER: _ClassVar[int]
    change_request: ChangeRequest
    def __init__(self, change_request: _Optional[_Union[ChangeRequest, _Mapping]] = ...) -> None: ...

class RejectChangeRequestRequest(_message.Message):
    __slots__ = ("chg_id", "reason")
    CHG_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    chg_id: str
    reason: str
    def __init__(self, chg_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class RejectChangeRequestResponse(_message.Message):
    __slots__ = ("change_request",)
    CHANGE_REQUEST_FIELD_NUMBER: _ClassVar[int]
    change_request: ChangeRequest
    def __init__(self, change_request: _Optional[_Union[ChangeRequest, _Mapping]] = ...) -> None: ...

class Release(_message.Message):
    __slots__ = ("release_id", "object", "scope", "version", "from_version_id", "source", "source_ref", "gate_id", "status", "requested_by", "requested_at", "approved_by", "approved_at", "rolled_back_by", "rolled_back_at", "reason")
    RELEASE_ID_FIELD_NUMBER: _ClassVar[int]
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    FROM_VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_REF_FIELD_NUMBER: _ClassVar[int]
    GATE_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_BY_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_AT_FIELD_NUMBER: _ClassVar[int]
    APPROVED_BY_FIELD_NUMBER: _ClassVar[int]
    APPROVED_AT_FIELD_NUMBER: _ClassVar[int]
    ROLLED_BACK_BY_FIELD_NUMBER: _ClassVar[int]
    ROLLED_BACK_AT_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    release_id: str
    object: str
    scope: str
    version: ConfigVersion
    from_version_id: str
    source: str
    source_ref: str
    gate_id: str
    status: str
    requested_by: str
    requested_at: _timestamp_pb2.Timestamp
    approved_by: str
    approved_at: _timestamp_pb2.Timestamp
    rolled_back_by: str
    rolled_back_at: _timestamp_pb2.Timestamp
    reason: str
    def __init__(self, release_id: _Optional[str] = ..., object: _Optional[str] = ..., scope: _Optional[str] = ..., version: _Optional[_Union[ConfigVersion, _Mapping]] = ..., from_version_id: _Optional[str] = ..., source: _Optional[str] = ..., source_ref: _Optional[str] = ..., gate_id: _Optional[str] = ..., status: _Optional[str] = ..., requested_by: _Optional[str] = ..., requested_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., approved_by: _Optional[str] = ..., approved_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., rolled_back_by: _Optional[str] = ..., rolled_back_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., reason: _Optional[str] = ...) -> None: ...

class CreateReleaseRequest(_message.Message):
    __slots__ = ("object", "scope", "content", "source_ref", "gate_id", "reason")
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    SOURCE_REF_FIELD_NUMBER: _ClassVar[int]
    GATE_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    object: str
    scope: str
    content: str
    source_ref: str
    gate_id: str
    reason: str
    def __init__(self, object: _Optional[str] = ..., scope: _Optional[str] = ..., content: _Optional[str] = ..., source_ref: _Optional[str] = ..., gate_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class CreateReleaseResponse(_message.Message):
    __slots__ = ("release",)
    RELEASE_FIELD_NUMBER: _ClassVar[int]
    release: Release
    def __init__(self, release: _Optional[_Union[Release, _Mapping]] = ...) -> None: ...

class ApproveReleaseRequest(_message.Message):
    __slots__ = ("release_id",)
    RELEASE_ID_FIELD_NUMBER: _ClassVar[int]
    release_id: str
    def __init__(self, release_id: _Optional[str] = ...) -> None: ...

class ApproveReleaseResponse(_message.Message):
    __slots__ = ("release",)
    RELEASE_FIELD_NUMBER: _ClassVar[int]
    release: Release
    def __init__(self, release: _Optional[_Union[Release, _Mapping]] = ...) -> None: ...

class GetReleaseRequest(_message.Message):
    __slots__ = ("release_id",)
    RELEASE_ID_FIELD_NUMBER: _ClassVar[int]
    release_id: str
    def __init__(self, release_id: _Optional[str] = ...) -> None: ...

class GetReleaseResponse(_message.Message):
    __slots__ = ("release",)
    RELEASE_FIELD_NUMBER: _ClassVar[int]
    release: Release
    def __init__(self, release: _Optional[_Union[Release, _Mapping]] = ...) -> None: ...

class RollbackReleaseRequest(_message.Message):
    __slots__ = ("release_id", "reason")
    RELEASE_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    release_id: str
    reason: str
    def __init__(self, release_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class RollbackReleaseResponse(_message.Message):
    __slots__ = ("release",)
    RELEASE_FIELD_NUMBER: _ClassVar[int]
    release: Release
    def __init__(self, release: _Optional[_Union[Release, _Mapping]] = ...) -> None: ...
