import datetime

from bambuflow.answer.v1 import answer_pb2 as _answer_pb2
from bambuflow.common.v1 import types_pb2 as _types_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class BatchStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    BATCH_STATUS_UNSPECIFIED: _ClassVar[BatchStatus]
    BATCH_STATUS_QUEUED: _ClassVar[BatchStatus]
    BATCH_STATUS_RUNNING: _ClassVar[BatchStatus]
    BATCH_STATUS_COMPLETED: _ClassVar[BatchStatus]
    BATCH_STATUS_FAILED: _ClassVar[BatchStatus]
    BATCH_STATUS_CANCELLED: _ClassVar[BatchStatus]

class BatchMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    BATCH_MODE_UNSPECIFIED: _ClassVar[BatchMode]
    BATCH_MODE_ANSWER: _ClassVar[BatchMode]
    BATCH_MODE_EVIDENCE_ONLY: _ClassVar[BatchMode]
BATCH_STATUS_UNSPECIFIED: BatchStatus
BATCH_STATUS_QUEUED: BatchStatus
BATCH_STATUS_RUNNING: BatchStatus
BATCH_STATUS_COMPLETED: BatchStatus
BATCH_STATUS_FAILED: BatchStatus
BATCH_STATUS_CANCELLED: BatchStatus
BATCH_MODE_UNSPECIFIED: BatchMode
BATCH_MODE_ANSWER: BatchMode
BATCH_MODE_EVIDENCE_ONLY: BatchMode

class CreateBatchRequest(_message.Message):
    __slots__ = ("batch_id", "delegation", "mode", "items", "idempotency_key", "request_digest")
    BATCH_ID_FIELD_NUMBER: _ClassVar[int]
    DELEGATION_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    REQUEST_DIGEST_FIELD_NUMBER: _ClassVar[int]
    batch_id: str
    delegation: str
    mode: BatchMode
    items: _containers.RepeatedCompositeFieldContainer[BatchItem]
    idempotency_key: str
    request_digest: str
    def __init__(self, batch_id: _Optional[str] = ..., delegation: _Optional[str] = ..., mode: _Optional[_Union[BatchMode, str]] = ..., items: _Optional[_Iterable[_Union[BatchItem, _Mapping]]] = ..., idempotency_key: _Optional[str] = ..., request_digest: _Optional[str] = ...) -> None: ...

class CreateBatchResponse(_message.Message):
    __slots__ = ("batch",)
    BATCH_FIELD_NUMBER: _ClassVar[int]
    batch: Batch
    def __init__(self, batch: _Optional[_Union[Batch, _Mapping]] = ...) -> None: ...

class BatchItem(_message.Message):
    __slots__ = ("custom_id", "question", "filters")
    CUSTOM_ID_FIELD_NUMBER: _ClassVar[int]
    QUESTION_FIELD_NUMBER: _ClassVar[int]
    FILTERS_FIELD_NUMBER: _ClassVar[int]
    custom_id: str
    question: str
    filters: _types_pb2.Filters
    def __init__(self, custom_id: _Optional[str] = ..., question: _Optional[str] = ..., filters: _Optional[_Union[_types_pb2.Filters, _Mapping]] = ...) -> None: ...

class GetBatchRequest(_message.Message):
    __slots__ = ("batch_id",)
    BATCH_ID_FIELD_NUMBER: _ClassVar[int]
    batch_id: str
    def __init__(self, batch_id: _Optional[str] = ...) -> None: ...

class GetBatchResponse(_message.Message):
    __slots__ = ("batch",)
    BATCH_FIELD_NUMBER: _ClassVar[int]
    batch: Batch
    def __init__(self, batch: _Optional[_Union[Batch, _Mapping]] = ...) -> None: ...

class CancelBatchRequest(_message.Message):
    __slots__ = ("batch_id",)
    BATCH_ID_FIELD_NUMBER: _ClassVar[int]
    batch_id: str
    def __init__(self, batch_id: _Optional[str] = ...) -> None: ...

class CancelBatchResponse(_message.Message):
    __slots__ = ("batch",)
    BATCH_FIELD_NUMBER: _ClassVar[int]
    batch: Batch
    def __init__(self, batch: _Optional[_Union[Batch, _Mapping]] = ...) -> None: ...

class ListResultsRequest(_message.Message):
    __slots__ = ("batch_id", "page_size", "page_token")
    BATCH_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    batch_id: str
    page_size: int
    page_token: str
    def __init__(self, batch_id: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListResultsResponse(_message.Message):
    __slots__ = ("results", "next_page_token")
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[BatchResult]
    next_page_token: str
    def __init__(self, results: _Optional[_Iterable[_Union[BatchResult, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class Batch(_message.Message):
    __slots__ = ("batch_id", "status", "mode", "counts", "created_at", "started_at", "completed_at", "expires_at")
    BATCH_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    COUNTS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    STARTED_AT_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    batch_id: str
    status: BatchStatus
    mode: BatchMode
    counts: BatchCounts
    created_at: _timestamp_pb2.Timestamp
    started_at: _timestamp_pb2.Timestamp
    completed_at: _timestamp_pb2.Timestamp
    expires_at: _timestamp_pb2.Timestamp
    def __init__(self, batch_id: _Optional[str] = ..., status: _Optional[_Union[BatchStatus, str]] = ..., mode: _Optional[_Union[BatchMode, str]] = ..., counts: _Optional[_Union[BatchCounts, _Mapping]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., started_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class BatchCounts(_message.Message):
    __slots__ = ("total", "succeeded", "failed", "cancelled")
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    SUCCEEDED_FIELD_NUMBER: _ClassVar[int]
    FAILED_FIELD_NUMBER: _ClassVar[int]
    CANCELLED_FIELD_NUMBER: _ClassVar[int]
    total: int
    succeeded: int
    failed: int
    cancelled: int
    def __init__(self, total: _Optional[int] = ..., succeeded: _Optional[int] = ..., failed: _Optional[int] = ..., cancelled: _Optional[int] = ...) -> None: ...

class BatchResult(_message.Message):
    __slots__ = ("custom_id", "answer", "error")
    CUSTOM_ID_FIELD_NUMBER: _ClassVar[int]
    ANSWER_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    custom_id: str
    answer: _answer_pb2.AnswerResponse
    error: ItemError
    def __init__(self, custom_id: _Optional[str] = ..., answer: _Optional[_Union[_answer_pb2.AnswerResponse, _Mapping]] = ..., error: _Optional[_Union[ItemError, _Mapping]] = ...) -> None: ...

class ItemError(_message.Message):
    __slots__ = ("type", "title")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    type: str
    title: str
    def __init__(self, type: _Optional[str] = ..., title: _Optional[str] = ...) -> None: ...
