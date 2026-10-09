import datetime

from bambuflow.admin.v1 import index_version_admin_pb2 as _index_version_admin_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class StartRebuildRequest(_message.Message):
    __slots__ = ("embedding_model", "chunker_version", "reason")
    EMBEDDING_MODEL_FIELD_NUMBER: _ClassVar[int]
    CHUNKER_VERSION_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    embedding_model: str
    chunker_version: str
    reason: str
    def __init__(self, embedding_model: _Optional[str] = ..., chunker_version: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class RebuildOperation(_message.Message):
    __slots__ = ("operation_id", "embedding_model", "chunker_version", "live_version", "reason", "requested_by", "requested_at", "status", "approved_by", "approved_at", "failure_reason", "expires_at", "rebuild_id")
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    EMBEDDING_MODEL_FIELD_NUMBER: _ClassVar[int]
    CHUNKER_VERSION_FIELD_NUMBER: _ClassVar[int]
    LIVE_VERSION_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_BY_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_AT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    APPROVED_BY_FIELD_NUMBER: _ClassVar[int]
    APPROVED_AT_FIELD_NUMBER: _ClassVar[int]
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    REBUILD_ID_FIELD_NUMBER: _ClassVar[int]
    operation_id: str
    embedding_model: str
    chunker_version: str
    live_version: str
    reason: str
    requested_by: str
    requested_at: _timestamp_pb2.Timestamp
    status: _index_version_admin_pb2.OperationStatus
    approved_by: str
    approved_at: _timestamp_pb2.Timestamp
    failure_reason: str
    expires_at: _timestamp_pb2.Timestamp
    rebuild_id: str
    def __init__(self, operation_id: _Optional[str] = ..., embedding_model: _Optional[str] = ..., chunker_version: _Optional[str] = ..., live_version: _Optional[str] = ..., reason: _Optional[str] = ..., requested_by: _Optional[str] = ..., requested_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., status: _Optional[_Union[_index_version_admin_pb2.OperationStatus, str]] = ..., approved_by: _Optional[str] = ..., approved_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., failure_reason: _Optional[str] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., rebuild_id: _Optional[str] = ...) -> None: ...

class StartRebuildResponse(_message.Message):
    __slots__ = ("operation", "created")
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    CREATED_FIELD_NUMBER: _ClassVar[int]
    operation: RebuildOperation
    created: bool
    def __init__(self, operation: _Optional[_Union[RebuildOperation, _Mapping]] = ..., created: _Optional[bool] = ...) -> None: ...

class ApproveRebuildRequest(_message.Message):
    __slots__ = ("operation_id",)
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    operation_id: str
    def __init__(self, operation_id: _Optional[str] = ...) -> None: ...

class ApproveRebuildResponse(_message.Message):
    __slots__ = ("operation",)
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    operation: RebuildOperation
    def __init__(self, operation: _Optional[_Union[RebuildOperation, _Mapping]] = ...) -> None: ...

class Rebuild(_message.Message):
    __slots__ = ("rebuild_id", "version", "embedding_model", "chunker_version", "status", "written_docs", "total_docs", "requested_by", "approved_by", "started_at", "finished_at", "cancelled_by", "reason")
    REBUILD_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    EMBEDDING_MODEL_FIELD_NUMBER: _ClassVar[int]
    CHUNKER_VERSION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    WRITTEN_DOCS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_DOCS_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_BY_FIELD_NUMBER: _ClassVar[int]
    APPROVED_BY_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    FINISHED_AT_FIELD_NUMBER: _ClassVar[int]
    CANCELLED_BY_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    rebuild_id: str
    version: str
    embedding_model: str
    chunker_version: str
    status: str
    written_docs: int
    total_docs: int
    requested_by: str
    approved_by: str
    started_at: _timestamp_pb2.Timestamp
    finished_at: _timestamp_pb2.Timestamp
    cancelled_by: str
    reason: str
    def __init__(self, rebuild_id: _Optional[str] = ..., version: _Optional[str] = ..., embedding_model: _Optional[str] = ..., chunker_version: _Optional[str] = ..., status: _Optional[str] = ..., written_docs: _Optional[int] = ..., total_docs: _Optional[int] = ..., requested_by: _Optional[str] = ..., approved_by: _Optional[str] = ..., started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., finished_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., cancelled_by: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class GetRebuildRequest(_message.Message):
    __slots__ = ("rebuild_id",)
    REBUILD_ID_FIELD_NUMBER: _ClassVar[int]
    rebuild_id: str
    def __init__(self, rebuild_id: _Optional[str] = ...) -> None: ...

class GetRebuildResponse(_message.Message):
    __slots__ = ("rebuild",)
    REBUILD_FIELD_NUMBER: _ClassVar[int]
    rebuild: Rebuild
    def __init__(self, rebuild: _Optional[_Union[Rebuild, _Mapping]] = ...) -> None: ...

class CancelRebuildRequest(_message.Message):
    __slots__ = ("rebuild_id", "reason")
    REBUILD_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    rebuild_id: str
    reason: str
    def __init__(self, rebuild_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class CancelRebuildResponse(_message.Message):
    __slots__ = ("rebuild",)
    REBUILD_FIELD_NUMBER: _ClassVar[int]
    rebuild: Rebuild
    def __init__(self, rebuild: _Optional[_Union[Rebuild, _Mapping]] = ...) -> None: ...
