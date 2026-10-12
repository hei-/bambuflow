import datetime

from bambuflow.common.v1 import types_pb2 as _types_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PushedDocument(_message.Message):
    __slots__ = ("doc_id", "title", "mime_type", "text", "raw", "acl_tokens", "classification", "source_updated_at", "source_uri")
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    MIME_TYPE_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    RAW_FIELD_NUMBER: _ClassVar[int]
    ACL_TOKENS_FIELD_NUMBER: _ClassVar[int]
    CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
    SOURCE_UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    SOURCE_URI_FIELD_NUMBER: _ClassVar[int]
    doc_id: str
    title: str
    mime_type: str
    text: str
    raw: bytes
    acl_tokens: _containers.RepeatedScalarFieldContainer[str]
    classification: _types_pb2.Classification
    source_updated_at: _timestamp_pb2.Timestamp
    source_uri: str
    def __init__(self, doc_id: _Optional[str] = ..., title: _Optional[str] = ..., mime_type: _Optional[str] = ..., text: _Optional[str] = ..., raw: _Optional[bytes] = ..., acl_tokens: _Optional[_Iterable[str]] = ..., classification: _Optional[_Union[_types_pb2.Classification, str]] = ..., source_updated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., source_uri: _Optional[str] = ...) -> None: ...

class PutDocumentRequest(_message.Message):
    __slots__ = ("source_id", "document")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    document: PushedDocument
    def __init__(self, source_id: _Optional[str] = ..., document: _Optional[_Union[PushedDocument, _Mapping]] = ...) -> None: ...

class PutDocumentResponse(_message.Message):
    __slots__ = ("event_id", "duplicate")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    DUPLICATE_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    duplicate: bool
    def __init__(self, event_id: _Optional[str] = ..., duplicate: _Optional[bool] = ...) -> None: ...

class DeleteDocumentRequest(_message.Message):
    __slots__ = ("source_id", "doc_id")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    doc_id: str
    def __init__(self, source_id: _Optional[str] = ..., doc_id: _Optional[str] = ...) -> None: ...

class DeleteDocumentResponse(_message.Message):
    __slots__ = ("event_id",)
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    def __init__(self, event_id: _Optional[str] = ...) -> None: ...

class BatchItem(_message.Message):
    __slots__ = ("upsert", "delete")
    UPSERT_FIELD_NUMBER: _ClassVar[int]
    DELETE_FIELD_NUMBER: _ClassVar[int]
    upsert: PushedDocument
    delete: str
    def __init__(self, upsert: _Optional[_Union[PushedDocument, _Mapping]] = ..., delete: _Optional[str] = ...) -> None: ...

class BatchDocumentsRequest(_message.Message):
    __slots__ = ("source_id", "items")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    items: _containers.RepeatedCompositeFieldContainer[BatchItem]
    def __init__(self, source_id: _Optional[str] = ..., items: _Optional[_Iterable[_Union[BatchItem, _Mapping]]] = ...) -> None: ...

class BatchItemResult(_message.Message):
    __slots__ = ("doc_id", "event_id", "duplicate", "error_type", "error_title")
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    DUPLICATE_FIELD_NUMBER: _ClassVar[int]
    ERROR_TYPE_FIELD_NUMBER: _ClassVar[int]
    ERROR_TITLE_FIELD_NUMBER: _ClassVar[int]
    doc_id: str
    event_id: str
    duplicate: bool
    error_type: str
    error_title: str
    def __init__(self, doc_id: _Optional[str] = ..., event_id: _Optional[str] = ..., duplicate: _Optional[bool] = ..., error_type: _Optional[str] = ..., error_title: _Optional[str] = ...) -> None: ...

class BatchDocumentsResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[BatchItemResult]
    def __init__(self, results: _Optional[_Iterable[_Union[BatchItemResult, _Mapping]]] = ...) -> None: ...
