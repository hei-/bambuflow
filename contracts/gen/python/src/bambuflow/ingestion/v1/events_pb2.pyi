import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
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

class DocumentChanged(_message.Message):
    __slots__ = ("event_id", "source_id", "doc_id", "change_type", "content_hash", "acl_fingerprint", "occurred_at", "detected_at", "detection", "raw_ref")
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
    def __init__(self, event_id: _Optional[str] = ..., source_id: _Optional[str] = ..., doc_id: _Optional[str] = ..., change_type: _Optional[_Union[ChangeType, str]] = ..., content_hash: _Optional[str] = ..., acl_fingerprint: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., detected_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., detection: _Optional[_Union[Detection, str]] = ..., raw_ref: _Optional[str] = ...) -> None: ...
