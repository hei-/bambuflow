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

class ChangeType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CHANGE_TYPE_UNSPECIFIED: _ClassVar[ChangeType]
    CHANGE_TYPE_UPSERT: _ClassVar[ChangeType]
    CHANGE_TYPE_DELETE: _ClassVar[ChangeType]
    CHANGE_TYPE_ACL_ONLY: _ClassVar[ChangeType]

class Detection(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DETECTION_UNSPECIFIED: _ClassVar[Detection]
    DETECTION_WEBHOOK: _ClassVar[Detection]
    DETECTION_POLL: _ClassVar[Detection]
    DETECTION_LISTING: _ClassVar[Detection]
    DETECTION_RECONCILE: _ClassVar[Detection]
    DETECTION_PUSH: _ClassVar[Detection]

class RefetchReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    REFETCH_REASON_UNSPECIFIED: _ClassVar[RefetchReason]
    REFETCH_REASON_TOMBSTONE_REVOKED: _ClassVar[RefetchReason]
    REFETCH_REASON_FULL_REBUILD: _ClassVar[RefetchReason]
CHANGE_TYPE_UNSPECIFIED: ChangeType
CHANGE_TYPE_UPSERT: ChangeType
CHANGE_TYPE_DELETE: ChangeType
CHANGE_TYPE_ACL_ONLY: ChangeType
DETECTION_UNSPECIFIED: Detection
DETECTION_WEBHOOK: Detection
DETECTION_POLL: Detection
DETECTION_LISTING: Detection
DETECTION_RECONCILE: Detection
DETECTION_PUSH: Detection
REFETCH_REASON_UNSPECIFIED: RefetchReason
REFETCH_REASON_TOMBSTONE_REVOKED: RefetchReason
REFETCH_REASON_FULL_REBUILD: RefetchReason

class DocumentChanged(_message.Message):
    __slots__ = ("event_id", "source_id", "doc_id", "change_type", "content_hash", "acl_fingerprint", "occurred_at", "detected_at", "detection", "raw_ref", "acl_tokens", "classification")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    CHANGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    ACL_FINGERPRINT_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    DETECTED_AT_FIELD_NUMBER: _ClassVar[int]
    DETECTION_FIELD_NUMBER: _ClassVar[int]
    RAW_REF_FIELD_NUMBER: _ClassVar[int]
    ACL_TOKENS_FIELD_NUMBER: _ClassVar[int]
    CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    source_id: str
    doc_id: str
    change_type: ChangeType
    content_hash: str
    acl_fingerprint: str
    occurred_at: _timestamp_pb2.Timestamp
    detected_at: _timestamp_pb2.Timestamp
    detection: Detection
    raw_ref: str
    acl_tokens: _containers.RepeatedScalarFieldContainer[str]
    classification: _types_pb2.Classification
    def __init__(self, event_id: _Optional[str] = ..., source_id: _Optional[str] = ..., doc_id: _Optional[str] = ..., change_type: _Optional[_Union[ChangeType, str]] = ..., content_hash: _Optional[str] = ..., acl_fingerprint: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., detected_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., detection: _Optional[_Union[Detection, str]] = ..., raw_ref: _Optional[str] = ..., acl_tokens: _Optional[_Iterable[str]] = ..., classification: _Optional[_Union[_types_pb2.Classification, str]] = ...) -> None: ...

class PartitionGrantChanged(_message.Message):
    __slots__ = ("event_id", "source_id", "partition", "grantees", "occurred_at")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    PARTITION_FIELD_NUMBER: _ClassVar[int]
    GRANTEES_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    source_id: str
    partition: str
    grantees: _containers.RepeatedScalarFieldContainer[str]
    occurred_at: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., source_id: _Optional[str] = ..., partition: _Optional[str] = ..., grantees: _Optional[_Iterable[str]] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class DocumentPurged(_message.Message):
    __slots__ = ("event_id", "source_id", "doc_id", "receipt_id", "occurred_at")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    RECEIPT_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    source_id: str
    doc_id: str
    receipt_id: str
    occurred_at: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., source_id: _Optional[str] = ..., doc_id: _Optional[str] = ..., receipt_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class RefetchRequested(_message.Message):
    __slots__ = ("event_id", "source_id", "doc_ids", "reason", "occurred_at")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    DOC_IDS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    source_id: str
    doc_ids: _containers.RepeatedScalarFieldContainer[str]
    reason: RefetchReason
    occurred_at: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., source_id: _Optional[str] = ..., doc_ids: _Optional[_Iterable[str]] = ..., reason: _Optional[_Union[RefetchReason, str]] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
