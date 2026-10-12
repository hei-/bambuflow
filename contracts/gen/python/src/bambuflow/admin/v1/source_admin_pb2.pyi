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

class PermissionModel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PERMISSION_MODEL_UNSPECIFIED: _ClassVar[PermissionModel]
    PERMISSION_MODEL_DOCUMENT: _ClassVar[PermissionModel]
    PERMISSION_MODEL_PARTITION: _ClassVar[PermissionModel]

class ApprovalCapacity(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    APPROVAL_CAPACITY_UNSPECIFIED: _ClassVar[ApprovalCapacity]
    APPROVAL_CAPACITY_DATA_OWNER: _ClassVar[ApprovalCapacity]
    APPROVAL_CAPACITY_INFOSEC: _ClassVar[ApprovalCapacity]

class GrantApplicationStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    GRANT_APPLICATION_STATUS_UNSPECIFIED: _ClassVar[GrantApplicationStatus]
    GRANT_APPLICATION_STATUS_PENDING: _ClassVar[GrantApplicationStatus]
    GRANT_APPLICATION_STATUS_APPROVED: _ClassVar[GrantApplicationStatus]
    GRANT_APPLICATION_STATUS_REJECTED: _ClassVar[GrantApplicationStatus]
    GRANT_APPLICATION_STATUS_WITHDRAWN: _ClassVar[GrantApplicationStatus]
    GRANT_APPLICATION_STATUS_EXPIRED: _ClassVar[GrantApplicationStatus]
SOURCE_STATUS_UNSPECIFIED: SourceStatus
SOURCE_STATUS_PENDING: SourceStatus
SOURCE_STATUS_ACTIVE: SourceStatus
SOURCE_STATUS_REVOKED: SourceStatus
PERMISSION_MODEL_UNSPECIFIED: PermissionModel
PERMISSION_MODEL_DOCUMENT: PermissionModel
PERMISSION_MODEL_PARTITION: PermissionModel
APPROVAL_CAPACITY_UNSPECIFIED: ApprovalCapacity
APPROVAL_CAPACITY_DATA_OWNER: ApprovalCapacity
APPROVAL_CAPACITY_INFOSEC: ApprovalCapacity
GRANT_APPLICATION_STATUS_UNSPECIFIED: GrantApplicationStatus
GRANT_APPLICATION_STATUS_PENDING: GrantApplicationStatus
GRANT_APPLICATION_STATUS_APPROVED: GrantApplicationStatus
GRANT_APPLICATION_STATUS_REJECTED: GrantApplicationStatus
GRANT_APPLICATION_STATUS_WITHDRAWN: GrantApplicationStatus
GRANT_APPLICATION_STATUS_EXPIRED: GrantApplicationStatus

class Source(_message.Message):
    __slots__ = ("source_id", "type", "owner_dept", "default_classification", "description", "status", "created_by", "created_at", "approved_by", "approved_at", "approval_basis", "revoked_by", "revoked_at", "revoke_reason", "security_approved_by", "security_approved_at", "security_approval_basis", "push_client", "permission_model")
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
    SECURITY_APPROVED_BY_FIELD_NUMBER: _ClassVar[int]
    SECURITY_APPROVED_AT_FIELD_NUMBER: _ClassVar[int]
    SECURITY_APPROVAL_BASIS_FIELD_NUMBER: _ClassVar[int]
    PUSH_CLIENT_FIELD_NUMBER: _ClassVar[int]
    PERMISSION_MODEL_FIELD_NUMBER: _ClassVar[int]
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
    security_approved_by: str
    security_approved_at: _timestamp_pb2.Timestamp
    security_approval_basis: str
    push_client: str
    permission_model: PermissionModel
    def __init__(self, source_id: _Optional[str] = ..., type: _Optional[_Union[_types_pb2.SourceType, str]] = ..., owner_dept: _Optional[str] = ..., default_classification: _Optional[_Union[_types_pb2.Classification, str]] = ..., description: _Optional[str] = ..., status: _Optional[_Union[SourceStatus, str]] = ..., created_by: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., approved_by: _Optional[str] = ..., approved_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., approval_basis: _Optional[str] = ..., revoked_by: _Optional[str] = ..., revoked_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., revoke_reason: _Optional[str] = ..., security_approved_by: _Optional[str] = ..., security_approved_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., security_approval_basis: _Optional[str] = ..., push_client: _Optional[str] = ..., permission_model: _Optional[_Union[PermissionModel, str]] = ...) -> None: ...

class ListSourcesRequest(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: SourceStatus
    def __init__(self, status: _Optional[_Union[SourceStatus, str]] = ...) -> None: ...

class ListSourcesResponse(_message.Message):
    __slots__ = ("sources",)
    SOURCES_FIELD_NUMBER: _ClassVar[int]
    sources: _containers.RepeatedCompositeFieldContainer[Source]
    def __init__(self, sources: _Optional[_Iterable[_Union[Source, _Mapping]]] = ...) -> None: ...

class RegisterSourceRequest(_message.Message):
    __slots__ = ("type", "owner_dept", "default_classification", "description", "push_client", "permission_model")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    OWNER_DEPT_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    PUSH_CLIENT_FIELD_NUMBER: _ClassVar[int]
    PERMISSION_MODEL_FIELD_NUMBER: _ClassVar[int]
    type: _types_pb2.SourceType
    owner_dept: str
    default_classification: _types_pb2.Classification
    description: str
    push_client: str
    permission_model: PermissionModel
    def __init__(self, type: _Optional[_Union[_types_pb2.SourceType, str]] = ..., owner_dept: _Optional[str] = ..., default_classification: _Optional[_Union[_types_pb2.Classification, str]] = ..., description: _Optional[str] = ..., push_client: _Optional[str] = ..., permission_model: _Optional[_Union[PermissionModel, str]] = ...) -> None: ...

class RegisterSourceResponse(_message.Message):
    __slots__ = ("source",)
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    source: Source
    def __init__(self, source: _Optional[_Union[Source, _Mapping]] = ...) -> None: ...

class ApproveSourceRequest(_message.Message):
    __slots__ = ("source_id", "basis", "capacity")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    BASIS_FIELD_NUMBER: _ClassVar[int]
    CAPACITY_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    basis: str
    capacity: ApprovalCapacity
    def __init__(self, source_id: _Optional[str] = ..., basis: _Optional[str] = ..., capacity: _Optional[_Union[ApprovalCapacity, str]] = ...) -> None: ...

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
    __slots__ = ("source_id", "tenant", "granted_by", "granted_at", "application_id")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    GRANTED_BY_FIELD_NUMBER: _ClassVar[int]
    GRANTED_AT_FIELD_NUMBER: _ClassVar[int]
    APPLICATION_ID_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    tenant: str
    granted_by: str
    granted_at: _timestamp_pb2.Timestamp
    application_id: str
    def __init__(self, source_id: _Optional[str] = ..., tenant: _Optional[str] = ..., granted_by: _Optional[str] = ..., granted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., application_id: _Optional[str] = ...) -> None: ...

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

class GrantApplication(_message.Message):
    __slots__ = ("application_id", "source_id", "tenant", "reason", "status", "applied_by", "applied_at", "decided_by", "decided_at", "decision_note")
    APPLICATION_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    APPLIED_BY_FIELD_NUMBER: _ClassVar[int]
    APPLIED_AT_FIELD_NUMBER: _ClassVar[int]
    DECIDED_BY_FIELD_NUMBER: _ClassVar[int]
    DECIDED_AT_FIELD_NUMBER: _ClassVar[int]
    DECISION_NOTE_FIELD_NUMBER: _ClassVar[int]
    application_id: str
    source_id: str
    tenant: str
    reason: str
    status: GrantApplicationStatus
    applied_by: str
    applied_at: _timestamp_pb2.Timestamp
    decided_by: str
    decided_at: _timestamp_pb2.Timestamp
    decision_note: str
    def __init__(self, application_id: _Optional[str] = ..., source_id: _Optional[str] = ..., tenant: _Optional[str] = ..., reason: _Optional[str] = ..., status: _Optional[_Union[GrantApplicationStatus, str]] = ..., applied_by: _Optional[str] = ..., applied_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., decided_by: _Optional[str] = ..., decided_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., decision_note: _Optional[str] = ...) -> None: ...

class ApplyGrantRequest(_message.Message):
    __slots__ = ("source_id", "tenant", "reason")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    tenant: str
    reason: str
    def __init__(self, source_id: _Optional[str] = ..., tenant: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class ApplyGrantResponse(_message.Message):
    __slots__ = ("application",)
    APPLICATION_FIELD_NUMBER: _ClassVar[int]
    application: GrantApplication
    def __init__(self, application: _Optional[_Union[GrantApplication, _Mapping]] = ...) -> None: ...

class ListGrantApplicationsRequest(_message.Message):
    __slots__ = ("status",)
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: GrantApplicationStatus
    def __init__(self, status: _Optional[_Union[GrantApplicationStatus, str]] = ...) -> None: ...

class ListGrantApplicationsResponse(_message.Message):
    __slots__ = ("applications",)
    APPLICATIONS_FIELD_NUMBER: _ClassVar[int]
    applications: _containers.RepeatedCompositeFieldContainer[GrantApplication]
    def __init__(self, applications: _Optional[_Iterable[_Union[GrantApplication, _Mapping]]] = ...) -> None: ...

class ApproveGrantApplicationRequest(_message.Message):
    __slots__ = ("application_id", "basis")
    APPLICATION_ID_FIELD_NUMBER: _ClassVar[int]
    BASIS_FIELD_NUMBER: _ClassVar[int]
    application_id: str
    basis: str
    def __init__(self, application_id: _Optional[str] = ..., basis: _Optional[str] = ...) -> None: ...

class ApproveGrantApplicationResponse(_message.Message):
    __slots__ = ("application", "grant")
    APPLICATION_FIELD_NUMBER: _ClassVar[int]
    GRANT_FIELD_NUMBER: _ClassVar[int]
    application: GrantApplication
    grant: Grant
    def __init__(self, application: _Optional[_Union[GrantApplication, _Mapping]] = ..., grant: _Optional[_Union[Grant, _Mapping]] = ...) -> None: ...

class RejectGrantApplicationRequest(_message.Message):
    __slots__ = ("application_id", "reason")
    APPLICATION_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    application_id: str
    reason: str
    def __init__(self, application_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class RejectGrantApplicationResponse(_message.Message):
    __slots__ = ("application",)
    APPLICATION_FIELD_NUMBER: _ClassVar[int]
    application: GrantApplication
    def __init__(self, application: _Optional[_Union[GrantApplication, _Mapping]] = ...) -> None: ...
