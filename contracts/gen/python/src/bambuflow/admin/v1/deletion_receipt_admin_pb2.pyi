import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ListDeletionReceiptsRequest(_message.Message):
    __slots__ = ("doc_id", "source_id", "page_size", "page_token")
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    doc_id: str
    source_id: str
    page_size: int
    page_token: str
    def __init__(self, doc_id: _Optional[str] = ..., source_id: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class StoreDeletion(_message.Message):
    __slots__ = ("store", "completed_at")
    STORE_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    store: str
    completed_at: _timestamp_pb2.Timestamp
    def __init__(self, store: _Optional[str] = ..., completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class DeletionReceipt(_message.Message):
    __slots__ = ("receipt_id", "doc_id", "source_id", "tombstoned_at", "stores", "pending_stores", "created_at")
    RECEIPT_ID_FIELD_NUMBER: _ClassVar[int]
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    TOMBSTONED_AT_FIELD_NUMBER: _ClassVar[int]
    STORES_FIELD_NUMBER: _ClassVar[int]
    PENDING_STORES_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    receipt_id: str
    doc_id: str
    source_id: str
    tombstoned_at: _timestamp_pb2.Timestamp
    stores: _containers.RepeatedCompositeFieldContainer[StoreDeletion]
    pending_stores: _containers.RepeatedScalarFieldContainer[str]
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, receipt_id: _Optional[str] = ..., doc_id: _Optional[str] = ..., source_id: _Optional[str] = ..., tombstoned_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., stores: _Optional[_Iterable[_Union[StoreDeletion, _Mapping]]] = ..., pending_stores: _Optional[_Iterable[str]] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ListDeletionReceiptsResponse(_message.Message):
    __slots__ = ("receipts", "next_page_token")
    RECEIPTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    receipts: _containers.RepeatedCompositeFieldContainer[DeletionReceipt]
    next_page_token: str
    def __init__(self, receipts: _Optional[_Iterable[_Union[DeletionReceipt, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...
