import datetime

from bambuflow.admin.v1 import index_version_admin_pb2 as _index_version_admin_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RevokeTombstonesRequest(_message.Message):
    __slots__ = ("source_id", "tombstoned_since", "tombstoned_until", "doc_ids", "reason")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    TOMBSTONED_SINCE_FIELD_NUMBER: _ClassVar[int]
    TOMBSTONED_UNTIL_FIELD_NUMBER: _ClassVar[int]
    DOC_IDS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    tombstoned_since: _timestamp_pb2.Timestamp
    tombstoned_until: _timestamp_pb2.Timestamp
    doc_ids: _containers.RepeatedScalarFieldContainer[str]
    reason: str
    def __init__(self, source_id: _Optional[str] = ..., tombstoned_since: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tombstoned_until: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., doc_ids: _Optional[_Iterable[str]] = ..., reason: _Optional[str] = ...) -> None: ...

class TombstoneRevocation(_message.Message):
    __slots__ = ("operation_id", "source_id", "tombstoned_since", "tombstoned_until", "doc_ids", "reason", "matched_docs", "revoked_docs", "requested_by", "requested_at", "status", "approved_by", "approved_at", "failure_reason", "expires_at", "refetch_event_id")
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    TOMBSTONED_SINCE_FIELD_NUMBER: _ClassVar[int]
    TOMBSTONED_UNTIL_FIELD_NUMBER: _ClassVar[int]
    DOC_IDS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    MATCHED_DOCS_FIELD_NUMBER: _ClassVar[int]
    REVOKED_DOCS_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_BY_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_AT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    APPROVED_BY_FIELD_NUMBER: _ClassVar[int]
    APPROVED_AT_FIELD_NUMBER: _ClassVar[int]
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    REFETCH_EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    operation_id: str
    source_id: str
    tombstoned_since: _timestamp_pb2.Timestamp
    tombstoned_until: _timestamp_pb2.Timestamp
    doc_ids: _containers.RepeatedScalarFieldContainer[str]
    reason: str
    matched_docs: int
    revoked_docs: int
    requested_by: str
    requested_at: _timestamp_pb2.Timestamp
    status: _index_version_admin_pb2.OperationStatus
    approved_by: str
    approved_at: _timestamp_pb2.Timestamp
    failure_reason: str
    expires_at: _timestamp_pb2.Timestamp
    refetch_event_id: str
    def __init__(self, operation_id: _Optional[str] = ..., source_id: _Optional[str] = ..., tombstoned_since: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., tombstoned_until: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., doc_ids: _Optional[_Iterable[str]] = ..., reason: _Optional[str] = ..., matched_docs: _Optional[int] = ..., revoked_docs: _Optional[int] = ..., requested_by: _Optional[str] = ..., requested_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., status: _Optional[_Union[_index_version_admin_pb2.OperationStatus, str]] = ..., approved_by: _Optional[str] = ..., approved_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., failure_reason: _Optional[str] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., refetch_event_id: _Optional[str] = ...) -> None: ...

class RevokeTombstonesResponse(_message.Message):
    __slots__ = ("revocation", "created")
    REVOCATION_FIELD_NUMBER: _ClassVar[int]
    CREATED_FIELD_NUMBER: _ClassVar[int]
    revocation: TombstoneRevocation
    created: bool
    def __init__(self, revocation: _Optional[_Union[TombstoneRevocation, _Mapping]] = ..., created: _Optional[bool] = ...) -> None: ...

class ApproveRevocationRequest(_message.Message):
    __slots__ = ("operation_id",)
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    operation_id: str
    def __init__(self, operation_id: _Optional[str] = ...) -> None: ...

class ApproveRevocationResponse(_message.Message):
    __slots__ = ("revocation",)
    REVOCATION_FIELD_NUMBER: _ClassVar[int]
    revocation: TombstoneRevocation
    def __init__(self, revocation: _Optional[_Union[TombstoneRevocation, _Mapping]] = ...) -> None: ...
