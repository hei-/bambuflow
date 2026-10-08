import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class QueryAuditRequest(_message.Message):
    __slots__ = ("sub", "doc_id", "to", "page_size", "page_token")
    SUB_FIELD_NUMBER: _ClassVar[int]
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    FROM_FIELD_NUMBER: _ClassVar[int]
    TO_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    sub: str
    doc_id: str
    to: _timestamp_pb2.Timestamp
    page_size: int
    page_token: str
    def __init__(self, sub: _Optional[str] = ..., doc_id: _Optional[str] = ..., to: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ..., **kwargs) -> None: ...

class QueryAuditResponse(_message.Message):
    __slots__ = ("records", "next_page_token")
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    records: _containers.RepeatedCompositeFieldContainer[AuditRecord]
    next_page_token: str
    def __init__(self, records: _Optional[_Iterable[_Union[AuditRecord, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class AuditRecord(_message.Message):
    __slots__ = ("time", "trace_id", "answer_id", "batch_id", "sub", "tenant", "actor", "endpoint", "status", "reason", "latency_ms", "candidate_chunk_ids", "context_chunk_ids", "shown_chunk_ids", "listed_doc_ids", "matched_in", "admin_method", "admin_path")
    TIME_FIELD_NUMBER: _ClassVar[int]
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    ANSWER_ID_FIELD_NUMBER: _ClassVar[int]
    BATCH_ID_FIELD_NUMBER: _ClassVar[int]
    SUB_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    ENDPOINT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    LATENCY_MS_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_CHUNK_IDS_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_CHUNK_IDS_FIELD_NUMBER: _ClassVar[int]
    SHOWN_CHUNK_IDS_FIELD_NUMBER: _ClassVar[int]
    LISTED_DOC_IDS_FIELD_NUMBER: _ClassVar[int]
    MATCHED_IN_FIELD_NUMBER: _ClassVar[int]
    ADMIN_METHOD_FIELD_NUMBER: _ClassVar[int]
    ADMIN_PATH_FIELD_NUMBER: _ClassVar[int]
    time: _timestamp_pb2.Timestamp
    trace_id: str
    answer_id: str
    batch_id: str
    sub: str
    tenant: str
    actor: str
    endpoint: str
    status: int
    reason: str
    latency_ms: int
    candidate_chunk_ids: _containers.RepeatedScalarFieldContainer[str]
    context_chunk_ids: _containers.RepeatedScalarFieldContainer[str]
    shown_chunk_ids: _containers.RepeatedScalarFieldContainer[str]
    listed_doc_ids: _containers.RepeatedScalarFieldContainer[str]
    matched_in: _containers.RepeatedScalarFieldContainer[str]
    admin_method: str
    admin_path: str
    def __init__(self, time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., trace_id: _Optional[str] = ..., answer_id: _Optional[str] = ..., batch_id: _Optional[str] = ..., sub: _Optional[str] = ..., tenant: _Optional[str] = ..., actor: _Optional[str] = ..., endpoint: _Optional[str] = ..., status: _Optional[int] = ..., reason: _Optional[str] = ..., latency_ms: _Optional[int] = ..., candidate_chunk_ids: _Optional[_Iterable[str]] = ..., context_chunk_ids: _Optional[_Iterable[str]] = ..., shown_chunk_ids: _Optional[_Iterable[str]] = ..., listed_doc_ids: _Optional[_Iterable[str]] = ..., matched_in: _Optional[_Iterable[str]] = ..., admin_method: _Optional[str] = ..., admin_path: _Optional[str] = ...) -> None: ...
