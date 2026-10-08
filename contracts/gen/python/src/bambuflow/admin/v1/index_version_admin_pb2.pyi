import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class IndexVersionStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INDEX_VERSION_STATUS_UNSPECIFIED: _ClassVar[IndexVersionStatus]
    INDEX_VERSION_STATUS_PENDING: _ClassVar[IndexVersionStatus]
    INDEX_VERSION_STATUS_LIVE: _ClassVar[IndexVersionStatus]
    INDEX_VERSION_STATUS_RETIRED: _ClassVar[IndexVersionStatus]

class OperationStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OPERATION_STATUS_UNSPECIFIED: _ClassVar[OperationStatus]
    OPERATION_STATUS_PENDING: _ClassVar[OperationStatus]
    OPERATION_STATUS_EXECUTED: _ClassVar[OperationStatus]
    OPERATION_STATUS_FAILED: _ClassVar[OperationStatus]
    OPERATION_STATUS_EXPIRED: _ClassVar[OperationStatus]
INDEX_VERSION_STATUS_UNSPECIFIED: IndexVersionStatus
INDEX_VERSION_STATUS_PENDING: IndexVersionStatus
INDEX_VERSION_STATUS_LIVE: IndexVersionStatus
INDEX_VERSION_STATUS_RETIRED: IndexVersionStatus
OPERATION_STATUS_UNSPECIFIED: OperationStatus
OPERATION_STATUS_PENDING: OperationStatus
OPERATION_STATUS_EXECUTED: OperationStatus
OPERATION_STATUS_FAILED: OperationStatus
OPERATION_STATUS_EXPIRED: OperationStatus

class IndexVersion(_message.Message):
    __slots__ = ("version", "status", "embedding_model", "chunker_version", "doc_count", "missing_doc_count", "created_at", "published_at")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    EMBEDDING_MODEL_FIELD_NUMBER: _ClassVar[int]
    CHUNKER_VERSION_FIELD_NUMBER: _ClassVar[int]
    DOC_COUNT_FIELD_NUMBER: _ClassVar[int]
    MISSING_DOC_COUNT_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    PUBLISHED_AT_FIELD_NUMBER: _ClassVar[int]
    version: str
    status: IndexVersionStatus
    embedding_model: str
    chunker_version: str
    doc_count: int
    missing_doc_count: int
    created_at: _timestamp_pb2.Timestamp
    published_at: _timestamp_pb2.Timestamp
    def __init__(self, version: _Optional[str] = ..., status: _Optional[_Union[IndexVersionStatus, str]] = ..., embedding_model: _Optional[str] = ..., chunker_version: _Optional[str] = ..., doc_count: _Optional[int] = ..., missing_doc_count: _Optional[int] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., published_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ListIndexVersionsRequest(_message.Message):
    __slots__ = ("page_size", "page_token")
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    page_size: int
    page_token: str
    def __init__(self, page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListIndexVersionsResponse(_message.Message):
    __slots__ = ("versions", "next_page_token")
    VERSIONS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    versions: _containers.RepeatedCompositeFieldContainer[IndexVersion]
    next_page_token: str
    def __init__(self, versions: _Optional[_Iterable[_Union[IndexVersion, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class Operation(_message.Message):
    __slots__ = ("operation_id", "action", "target_version", "live_version", "reason", "requested_by", "requested_at", "status", "approved_by", "approved_at", "failure_reason", "expires_at")
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    TARGET_VERSION_FIELD_NUMBER: _ClassVar[int]
    LIVE_VERSION_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_BY_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_AT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    APPROVED_BY_FIELD_NUMBER: _ClassVar[int]
    APPROVED_AT_FIELD_NUMBER: _ClassVar[int]
    FAILURE_REASON_FIELD_NUMBER: _ClassVar[int]
    EXPIRES_AT_FIELD_NUMBER: _ClassVar[int]
    operation_id: str
    action: str
    target_version: str
    live_version: str
    reason: str
    requested_by: str
    requested_at: _timestamp_pb2.Timestamp
    status: OperationStatus
    approved_by: str
    approved_at: _timestamp_pb2.Timestamp
    failure_reason: str
    expires_at: _timestamp_pb2.Timestamp
    def __init__(self, operation_id: _Optional[str] = ..., action: _Optional[str] = ..., target_version: _Optional[str] = ..., live_version: _Optional[str] = ..., reason: _Optional[str] = ..., requested_by: _Optional[str] = ..., requested_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., status: _Optional[_Union[OperationStatus, str]] = ..., approved_by: _Optional[str] = ..., approved_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., failure_reason: _Optional[str] = ..., expires_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ActivateIndexVersionRequest(_message.Message):
    __slots__ = ("version", "reason")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    version: str
    reason: str
    def __init__(self, version: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class ActivateIndexVersionResponse(_message.Message):
    __slots__ = ("operation", "created")
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    CREATED_FIELD_NUMBER: _ClassVar[int]
    operation: Operation
    created: bool
    def __init__(self, operation: _Optional[_Union[Operation, _Mapping]] = ..., created: _Optional[bool] = ...) -> None: ...

class ApproveOperationRequest(_message.Message):
    __slots__ = ("operation_id",)
    OPERATION_ID_FIELD_NUMBER: _ClassVar[int]
    operation_id: str
    def __init__(self, operation_id: _Optional[str] = ...) -> None: ...

class ApproveOperationResponse(_message.Message):
    __slots__ = ("operation",)
    OPERATION_FIELD_NUMBER: _ClassVar[int]
    operation: Operation
    def __init__(self, operation: _Optional[_Union[Operation, _Mapping]] = ...) -> None: ...
