import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GetIngestStatusRequest(_message.Message):
    __slots__ = ("doc_id",)
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    doc_id: str
    def __init__(self, doc_id: _Optional[str] = ...) -> None: ...

class IngestError(_message.Message):
    __slots__ = ("code", "message")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    code: str
    message: str
    def __init__(self, code: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...

class GetIngestStatusResponse(_message.Message):
    __slots__ = ("doc_id", "source_id", "event_id", "stage", "state", "last_error", "attempts", "ingest_batch_id", "updated_at")
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_FIELD_NUMBER: _ClassVar[int]
    STATE_FIELD_NUMBER: _ClassVar[int]
    LAST_ERROR_FIELD_NUMBER: _ClassVar[int]
    ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    INGEST_BATCH_ID_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    doc_id: str
    source_id: str
    event_id: str
    stage: str
    state: str
    last_error: IngestError
    attempts: int
    ingest_batch_id: str
    updated_at: _timestamp_pb2.Timestamp
    def __init__(self, doc_id: _Optional[str] = ..., source_id: _Optional[str] = ..., event_id: _Optional[str] = ..., stage: _Optional[str] = ..., state: _Optional[str] = ..., last_error: _Optional[_Union[IngestError, _Mapping]] = ..., attempts: _Optional[int] = ..., ingest_batch_id: _Optional[str] = ..., updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class DeadLetter(_message.Message):
    __slots__ = ("doc_id", "source_id", "event_id", "stage", "error", "pipeline_version", "attempts", "replays", "quarantined", "first_failed_at", "last_failed_at")
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_VERSION_FIELD_NUMBER: _ClassVar[int]
    ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    REPLAYS_FIELD_NUMBER: _ClassVar[int]
    QUARANTINED_FIELD_NUMBER: _ClassVar[int]
    FIRST_FAILED_AT_FIELD_NUMBER: _ClassVar[int]
    LAST_FAILED_AT_FIELD_NUMBER: _ClassVar[int]
    doc_id: str
    source_id: str
    event_id: str
    stage: str
    error: IngestError
    pipeline_version: str
    attempts: int
    replays: int
    quarantined: bool
    first_failed_at: _timestamp_pb2.Timestamp
    last_failed_at: _timestamp_pb2.Timestamp
    def __init__(self, doc_id: _Optional[str] = ..., source_id: _Optional[str] = ..., event_id: _Optional[str] = ..., stage: _Optional[str] = ..., error: _Optional[_Union[IngestError, _Mapping]] = ..., pipeline_version: _Optional[str] = ..., attempts: _Optional[int] = ..., replays: _Optional[int] = ..., quarantined: _Optional[bool] = ..., first_failed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., last_failed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ListDeadLettersRequest(_message.Message):
    __slots__ = ("source_id", "stage", "error_code", "quarantined_only", "page_size", "page_token")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    QUARANTINED_ONLY_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    stage: str
    error_code: str
    quarantined_only: bool
    page_size: int
    page_token: str
    def __init__(self, source_id: _Optional[str] = ..., stage: _Optional[str] = ..., error_code: _Optional[str] = ..., quarantined_only: _Optional[bool] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListDeadLettersResponse(_message.Message):
    __slots__ = ("dead_letters", "next_page_token")
    DEAD_LETTERS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    dead_letters: _containers.RepeatedCompositeFieldContainer[DeadLetter]
    next_page_token: str
    def __init__(self, dead_letters: _Optional[_Iterable[_Union[DeadLetter, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class ReplayRequest(_message.Message):
    __slots__ = ("source_id", "stage", "error_code", "doc_ids", "reason", "to", "pipeline_version", "include_succeeded")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_CODE_FIELD_NUMBER: _ClassVar[int]
    DOC_IDS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    FROM_FIELD_NUMBER: _ClassVar[int]
    TO_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_VERSION_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_SUCCEEDED_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    stage: str
    error_code: str
    doc_ids: _containers.RepeatedScalarFieldContainer[str]
    reason: str
    to: _timestamp_pb2.Timestamp
    pipeline_version: str
    include_succeeded: bool
    def __init__(self, source_id: _Optional[str] = ..., stage: _Optional[str] = ..., error_code: _Optional[str] = ..., doc_ids: _Optional[_Iterable[str]] = ..., reason: _Optional[str] = ..., to: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., pipeline_version: _Optional[str] = ..., include_succeeded: _Optional[bool] = ..., **kwargs) -> None: ...

class ReplayResponse(_message.Message):
    __slots__ = ("ingest_batch_id", "replayed", "skipped_quarantined", "reprocessed")
    INGEST_BATCH_ID_FIELD_NUMBER: _ClassVar[int]
    REPLAYED_FIELD_NUMBER: _ClassVar[int]
    SKIPPED_QUARANTINED_FIELD_NUMBER: _ClassVar[int]
    REPROCESSED_FIELD_NUMBER: _ClassVar[int]
    ingest_batch_id: str
    replayed: int
    skipped_quarantined: int
    reprocessed: int
    def __init__(self, ingest_batch_id: _Optional[str] = ..., replayed: _Optional[int] = ..., skipped_quarantined: _Optional[int] = ..., reprocessed: _Optional[int] = ...) -> None: ...
