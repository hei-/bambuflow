from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Classification(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CLASSIFICATION_UNSPECIFIED: _ClassVar[Classification]
    CLASSIFICATION_PUBLIC: _ClassVar[Classification]
    CLASSIFICATION_INTERNAL: _ClassVar[Classification]
    CLASSIFICATION_CONFIDENTIAL: _ClassVar[Classification]
    CLASSIFICATION_RESTRICTED: _ClassVar[Classification]

class SourceType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SOURCE_TYPE_UNSPECIFIED: _ClassVar[SourceType]
    SOURCE_TYPE_WIKI: _ClassVar[SourceType]
    SOURCE_TYPE_MAIL: _ClassVar[SourceType]
    SOURCE_TYPE_FILE: _ClassVar[SourceType]
    SOURCE_TYPE_IMAGE: _ClassVar[SourceType]
    SOURCE_TYPE_TABLE: _ClassVar[SourceType]
    SOURCE_TYPE_LOG: _ClassVar[SourceType]
    SOURCE_TYPE_DB: _ClassVar[SourceType]

class ValidityStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    VALIDITY_STATUS_UNSPECIFIED: _ClassVar[ValidityStatus]
    VALIDITY_STATUS_EFFECTIVE: _ClassVar[ValidityStatus]
    VALIDITY_STATUS_PENDING: _ClassVar[ValidityStatus]
    VALIDITY_STATUS_REPEALED: _ClassVar[ValidityStatus]

class EntityType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ENTITY_TYPE_UNSPECIFIED: _ClassVar[EntityType]
    ENTITY_TYPE_ORG: _ClassVar[EntityType]
    ENTITY_TYPE_PRODUCT: _ClassVar[EntityType]
    ENTITY_TYPE_POLICY_CLAUSE: _ClassVar[EntityType]
    ENTITY_TYPE_SYSTEM: _ClassVar[EntityType]
    ENTITY_TYPE_ROLE: _ClassVar[EntityType]

class RelationType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RELATION_TYPE_UNSPECIFIED: _ClassVar[RelationType]
    RELATION_TYPE_DEPENDS_ON: _ClassVar[RelationType]
    RELATION_TYPE_BELONGS_TO: _ClassVar[RelationType]
    RELATION_TYPE_RESPONSIBLE_FOR: _ClassVar[RelationType]
    RELATION_TYPE_CITES: _ClassVar[RelationType]
    RELATION_TYPE_APPLIES_TO: _ClassVar[RelationType]
CLASSIFICATION_UNSPECIFIED: Classification
CLASSIFICATION_PUBLIC: Classification
CLASSIFICATION_INTERNAL: Classification
CLASSIFICATION_CONFIDENTIAL: Classification
CLASSIFICATION_RESTRICTED: Classification
SOURCE_TYPE_UNSPECIFIED: SourceType
SOURCE_TYPE_WIKI: SourceType
SOURCE_TYPE_MAIL: SourceType
SOURCE_TYPE_FILE: SourceType
SOURCE_TYPE_IMAGE: SourceType
SOURCE_TYPE_TABLE: SourceType
SOURCE_TYPE_LOG: SourceType
SOURCE_TYPE_DB: SourceType
VALIDITY_STATUS_UNSPECIFIED: ValidityStatus
VALIDITY_STATUS_EFFECTIVE: ValidityStatus
VALIDITY_STATUS_PENDING: ValidityStatus
VALIDITY_STATUS_REPEALED: ValidityStatus
ENTITY_TYPE_UNSPECIFIED: EntityType
ENTITY_TYPE_ORG: EntityType
ENTITY_TYPE_PRODUCT: EntityType
ENTITY_TYPE_POLICY_CLAUSE: EntityType
ENTITY_TYPE_SYSTEM: EntityType
ENTITY_TYPE_ROLE: EntityType
RELATION_TYPE_UNSPECIFIED: RelationType
RELATION_TYPE_DEPENDS_ON: RelationType
RELATION_TYPE_BELONGS_TO: RelationType
RELATION_TYPE_RESPONSIBLE_FOR: RelationType
RELATION_TYPE_CITES: RelationType
RELATION_TYPE_APPLIES_TO: RelationType

class Span(_message.Message):
    __slots__ = ("char_start", "char_end", "line_start", "line_end", "page", "bbox")
    CHAR_START_FIELD_NUMBER: _ClassVar[int]
    CHAR_END_FIELD_NUMBER: _ClassVar[int]
    LINE_START_FIELD_NUMBER: _ClassVar[int]
    LINE_END_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    BBOX_FIELD_NUMBER: _ClassVar[int]
    char_start: int
    char_end: int
    line_start: int
    line_end: int
    page: int
    bbox: BoundingBox
    def __init__(self, char_start: _Optional[int] = ..., char_end: _Optional[int] = ..., line_start: _Optional[int] = ..., line_end: _Optional[int] = ..., page: _Optional[int] = ..., bbox: _Optional[_Union[BoundingBox, _Mapping]] = ...) -> None: ...

class BoundingBox(_message.Message):
    __slots__ = ("x0", "y0", "x1", "y1")
    X0_FIELD_NUMBER: _ClassVar[int]
    Y0_FIELD_NUMBER: _ClassVar[int]
    X1_FIELD_NUMBER: _ClassVar[int]
    Y1_FIELD_NUMBER: _ClassVar[int]
    x0: float
    y0: float
    x1: float
    y1: float
    def __init__(self, x0: _Optional[float] = ..., y0: _Optional[float] = ..., x1: _Optional[float] = ..., y1: _Optional[float] = ...) -> None: ...

class Filters(_message.Message):
    __slots__ = ("source_ids", "doc_types", "depts", "topic_codes", "effective_from", "effective_to", "validity_statuses")
    SOURCE_IDS_FIELD_NUMBER: _ClassVar[int]
    DOC_TYPES_FIELD_NUMBER: _ClassVar[int]
    DEPTS_FIELD_NUMBER: _ClassVar[int]
    TOPIC_CODES_FIELD_NUMBER: _ClassVar[int]
    EFFECTIVE_FROM_FIELD_NUMBER: _ClassVar[int]
    EFFECTIVE_TO_FIELD_NUMBER: _ClassVar[int]
    VALIDITY_STATUSES_FIELD_NUMBER: _ClassVar[int]
    source_ids: _containers.RepeatedScalarFieldContainer[str]
    doc_types: _containers.RepeatedScalarFieldContainer[str]
    depts: _containers.RepeatedScalarFieldContainer[str]
    topic_codes: _containers.RepeatedScalarFieldContainer[str]
    effective_from: str
    effective_to: str
    validity_statuses: _containers.RepeatedScalarFieldContainer[ValidityStatus]
    def __init__(self, source_ids: _Optional[_Iterable[str]] = ..., doc_types: _Optional[_Iterable[str]] = ..., depts: _Optional[_Iterable[str]] = ..., topic_codes: _Optional[_Iterable[str]] = ..., effective_from: _Optional[str] = ..., effective_to: _Optional[str] = ..., validity_statuses: _Optional[_Iterable[_Union[ValidityStatus, str]]] = ...) -> None: ...

class DocMetadata(_message.Message):
    __slots__ = ("doc_type", "dept", "topic_codes", "effective_date", "validity_status", "missing", "vocabulary_version", "superseded_by", "supersedes")
    DOC_TYPE_FIELD_NUMBER: _ClassVar[int]
    DEPT_FIELD_NUMBER: _ClassVar[int]
    TOPIC_CODES_FIELD_NUMBER: _ClassVar[int]
    EFFECTIVE_DATE_FIELD_NUMBER: _ClassVar[int]
    VALIDITY_STATUS_FIELD_NUMBER: _ClassVar[int]
    MISSING_FIELD_NUMBER: _ClassVar[int]
    VOCABULARY_VERSION_FIELD_NUMBER: _ClassVar[int]
    SUPERSEDED_BY_FIELD_NUMBER: _ClassVar[int]
    SUPERSEDES_FIELD_NUMBER: _ClassVar[int]
    doc_type: str
    dept: str
    topic_codes: _containers.RepeatedScalarFieldContainer[str]
    effective_date: str
    validity_status: ValidityStatus
    missing: _containers.RepeatedScalarFieldContainer[str]
    vocabulary_version: str
    superseded_by: str
    supersedes: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, doc_type: _Optional[str] = ..., dept: _Optional[str] = ..., topic_codes: _Optional[_Iterable[str]] = ..., effective_date: _Optional[str] = ..., validity_status: _Optional[_Union[ValidityStatus, str]] = ..., missing: _Optional[_Iterable[str]] = ..., vocabulary_version: _Optional[str] = ..., superseded_by: _Optional[str] = ..., supersedes: _Optional[_Iterable[str]] = ...) -> None: ...

class Degradation(_message.Message):
    __slots__ = ("stage", "reason")
    STAGE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    stage: str
    reason: str
    def __init__(self, stage: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...
