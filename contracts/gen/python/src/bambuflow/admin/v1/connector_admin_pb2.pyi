import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SyncRun(_message.Message):
    __slots__ = ("command", "started_at", "finished_at", "result", "error", "documents", "upserts", "acl_only", "deletes", "deletes_paused", "deletes_paused_reason", "refetched")
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    FINISHED_AT_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    DOCUMENTS_FIELD_NUMBER: _ClassVar[int]
    UPSERTS_FIELD_NUMBER: _ClassVar[int]
    ACL_ONLY_FIELD_NUMBER: _ClassVar[int]
    DELETES_FIELD_NUMBER: _ClassVar[int]
    DELETES_PAUSED_FIELD_NUMBER: _ClassVar[int]
    DELETES_PAUSED_REASON_FIELD_NUMBER: _ClassVar[int]
    REFETCHED_FIELD_NUMBER: _ClassVar[int]
    command: str
    started_at: _timestamp_pb2.Timestamp
    finished_at: _timestamp_pb2.Timestamp
    result: str
    error: str
    documents: int
    upserts: int
    acl_only: int
    deletes: int
    deletes_paused: bool
    deletes_paused_reason: str
    refetched: int
    def __init__(self, command: _Optional[str] = ..., started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., finished_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., result: _Optional[str] = ..., error: _Optional[str] = ..., documents: _Optional[int] = ..., upserts: _Optional[int] = ..., acl_only: _Optional[int] = ..., deletes: _Optional[int] = ..., deletes_paused: _Optional[bool] = ..., deletes_paused_reason: _Optional[str] = ..., refetched: _Optional[int] = ...) -> None: ...

class Pause(_message.Message):
    __slots__ = ("paused_by", "paused_at", "reason")
    PAUSED_BY_FIELD_NUMBER: _ClassVar[int]
    PAUSED_AT_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    paused_by: str
    paused_at: _timestamp_pb2.Timestamp
    reason: str
    def __init__(self, paused_by: _Optional[str] = ..., paused_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., reason: _Optional[str] = ...) -> None: ...

class GetSyncStatusRequest(_message.Message):
    __slots__ = ("source_id",)
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    def __init__(self, source_id: _Optional[str] = ...) -> None: ...

class GetSyncStatusResponse(_message.Message):
    __slots__ = ("source_id", "last_run", "last_success", "freshness_seconds", "pause")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    LAST_RUN_FIELD_NUMBER: _ClassVar[int]
    LAST_SUCCESS_FIELD_NUMBER: _ClassVar[int]
    FRESHNESS_SECONDS_FIELD_NUMBER: _ClassVar[int]
    PAUSE_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    last_run: SyncRun
    last_success: SyncRun
    freshness_seconds: int
    pause: Pause
    def __init__(self, source_id: _Optional[str] = ..., last_run: _Optional[_Union[SyncRun, _Mapping]] = ..., last_success: _Optional[_Union[SyncRun, _Mapping]] = ..., freshness_seconds: _Optional[int] = ..., pause: _Optional[_Union[Pause, _Mapping]] = ...) -> None: ...

class PauseSourceRequest(_message.Message):
    __slots__ = ("source_id", "reason")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    reason: str
    def __init__(self, source_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class PauseSourceResponse(_message.Message):
    __slots__ = ("pause", "changed")
    PAUSE_FIELD_NUMBER: _ClassVar[int]
    CHANGED_FIELD_NUMBER: _ClassVar[int]
    pause: Pause
    changed: bool
    def __init__(self, pause: _Optional[_Union[Pause, _Mapping]] = ..., changed: _Optional[bool] = ...) -> None: ...

class ResumeSourceRequest(_message.Message):
    __slots__ = ("source_id",)
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    def __init__(self, source_id: _Optional[str] = ...) -> None: ...

class ResumeSourceResponse(_message.Message):
    __slots__ = ("changed",)
    CHANGED_FIELD_NUMBER: _ClassVar[int]
    changed: bool
    def __init__(self, changed: _Optional[bool] = ...) -> None: ...

class DeletionHold(_message.Message):
    __slots__ = ("hold_id", "source_id", "doc_ids", "synced_docs", "detected_at", "status", "confirmed_by", "approved_by", "discarded_by", "reason", "applied_docs", "updated_at")
    HOLD_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    DOC_IDS_FIELD_NUMBER: _ClassVar[int]
    SYNCED_DOCS_FIELD_NUMBER: _ClassVar[int]
    DETECTED_AT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CONFIRMED_BY_FIELD_NUMBER: _ClassVar[int]
    APPROVED_BY_FIELD_NUMBER: _ClassVar[int]
    DISCARDED_BY_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    APPLIED_DOCS_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    hold_id: str
    source_id: str
    doc_ids: _containers.RepeatedScalarFieldContainer[str]
    synced_docs: int
    detected_at: _timestamp_pb2.Timestamp
    status: str
    confirmed_by: str
    approved_by: str
    discarded_by: str
    reason: str
    applied_docs: int
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, hold_id: _Optional[str] = ..., source_id: _Optional[str] = ..., doc_ids: _Optional[_Iterable[str]] = ..., synced_docs: _Optional[int] = ..., detected_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., status: _Optional[str] = ..., confirmed_by: _Optional[str] = ..., approved_by: _Optional[str] = ..., discarded_by: _Optional[str] = ..., reason: _Optional[str] = ..., applied_docs: _Optional[int] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ListPendingDeletionsRequest(_message.Message):
    __slots__ = ("source_id",)
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    def __init__(self, source_id: _Optional[str] = ...) -> None: ...

class ListPendingDeletionsResponse(_message.Message):
    __slots__ = ("holds",)
    HOLDS_FIELD_NUMBER: _ClassVar[int]
    holds: _containers.RepeatedCompositeFieldContainer[DeletionHold]
    def __init__(self, holds: _Optional[_Iterable[_Union[DeletionHold, _Mapping]]] = ...) -> None: ...

class ConfirmDeletionRequest(_message.Message):
    __slots__ = ("source_id", "hold_id", "reason")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    HOLD_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    hold_id: str
    reason: str
    def __init__(self, source_id: _Optional[str] = ..., hold_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class ConfirmDeletionResponse(_message.Message):
    __slots__ = ("hold",)
    HOLD_FIELD_NUMBER: _ClassVar[int]
    hold: DeletionHold
    def __init__(self, hold: _Optional[_Union[DeletionHold, _Mapping]] = ...) -> None: ...

class DiscardDeletionRequest(_message.Message):
    __slots__ = ("source_id", "hold_id", "reason")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    HOLD_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    hold_id: str
    reason: str
    def __init__(self, source_id: _Optional[str] = ..., hold_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class DiscardDeletionResponse(_message.Message):
    __slots__ = ("hold",)
    HOLD_FIELD_NUMBER: _ClassVar[int]
    hold: DeletionHold
    def __init__(self, hold: _Optional[_Union[DeletionHold, _Mapping]] = ...) -> None: ...
