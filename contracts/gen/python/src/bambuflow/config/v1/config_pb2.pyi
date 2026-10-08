from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TermStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TERM_STATUS_UNSPECIFIED: _ClassVar[TermStatus]
    TERM_STATUS_ACTIVE: _ClassVar[TermStatus]
    TERM_STATUS_DEPRECATED: _ClassVar[TermStatus]
TERM_STATUS_UNSPECIFIED: TermStatus
TERM_STATUS_ACTIVE: TermStatus
TERM_STATUS_DEPRECATED: TermStatus

class GetSnapshotRequest(_message.Message):
    __slots__ = ("service",)
    SERVICE_FIELD_NUMBER: _ClassVar[int]
    service: str
    def __init__(self, service: _Optional[str] = ...) -> None: ...

class GetSnapshotResponse(_message.Message):
    __slots__ = ("snapshot_version", "vocabularies", "source_grants", "experiments")
    SNAPSHOT_VERSION_FIELD_NUMBER: _ClassVar[int]
    VOCABULARIES_FIELD_NUMBER: _ClassVar[int]
    SOURCE_GRANTS_FIELD_NUMBER: _ClassVar[int]
    EXPERIMENTS_FIELD_NUMBER: _ClassVar[int]
    snapshot_version: str
    vocabularies: _containers.RepeatedCompositeFieldContainer[Vocabulary]
    source_grants: _containers.RepeatedCompositeFieldContainer[SourceGrant]
    experiments: _containers.RepeatedCompositeFieldContainer[RunningExperiment]
    def __init__(self, snapshot_version: _Optional[str] = ..., vocabularies: _Optional[_Iterable[_Union[Vocabulary, _Mapping]]] = ..., source_grants: _Optional[_Iterable[_Union[SourceGrant, _Mapping]]] = ..., experiments: _Optional[_Iterable[_Union[RunningExperiment, _Mapping]]] = ...) -> None: ...

class RunningExperiment(_message.Message):
    __slots__ = ("exp_id", "tenant", "traffic_percent", "support_threshold", "min_coverage")
    EXP_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    TRAFFIC_PERCENT_FIELD_NUMBER: _ClassVar[int]
    SUPPORT_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    MIN_COVERAGE_FIELD_NUMBER: _ClassVar[int]
    exp_id: str
    tenant: str
    traffic_percent: int
    support_threshold: float
    min_coverage: float
    def __init__(self, exp_id: _Optional[str] = ..., tenant: _Optional[str] = ..., traffic_percent: _Optional[int] = ..., support_threshold: _Optional[float] = ..., min_coverage: _Optional[float] = ...) -> None: ...

class SourceGrant(_message.Message):
    __slots__ = ("tenant", "source_id")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    source_id: str
    def __init__(self, tenant: _Optional[str] = ..., source_id: _Optional[str] = ...) -> None: ...

class Vocabulary(_message.Message):
    __slots__ = ("name", "terms")
    NAME_FIELD_NUMBER: _ClassVar[int]
    TERMS_FIELD_NUMBER: _ClassVar[int]
    name: str
    terms: _containers.RepeatedCompositeFieldContainer[VocabularyTerm]
    def __init__(self, name: _Optional[str] = ..., terms: _Optional[_Iterable[_Union[VocabularyTerm, _Mapping]]] = ...) -> None: ...

class VocabularyTerm(_message.Message):
    __slots__ = ("code", "name", "synonyms", "parent", "status", "replaced_by")
    CODE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SYNONYMS_FIELD_NUMBER: _ClassVar[int]
    PARENT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    REPLACED_BY_FIELD_NUMBER: _ClassVar[int]
    code: str
    name: str
    synonyms: _containers.RepeatedScalarFieldContainer[str]
    parent: str
    status: TermStatus
    replaced_by: str
    def __init__(self, code: _Optional[str] = ..., name: _Optional[str] = ..., synonyms: _Optional[_Iterable[str]] = ..., parent: _Optional[str] = ..., status: _Optional[_Union[TermStatus, str]] = ..., replaced_by: _Optional[str] = ...) -> None: ...
