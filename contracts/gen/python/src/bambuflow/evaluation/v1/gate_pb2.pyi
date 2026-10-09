import datetime

from google.protobuf import duration_pb2 as _duration_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Verdict(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    VERDICT_UNSPECIFIED: _ClassVar[Verdict]
    VERDICT_RUNNING: _ClassVar[Verdict]
    VERDICT_PASSED: _ClassVar[Verdict]
    VERDICT_FAILED: _ClassVar[Verdict]
    VERDICT_INDETERMINATE: _ClassVar[Verdict]
VERDICT_UNSPECIFIED: Verdict
VERDICT_RUNNING: Verdict
VERDICT_PASSED: Verdict
VERDICT_FAILED: Verdict
VERDICT_INDETERMINATE: Verdict

class EvalConfig(_message.Message):
    __slots__ = ("index_version", "snapshot_version")
    INDEX_VERSION_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_VERSION_FIELD_NUMBER: _ClassVar[int]
    index_version: str
    snapshot_version: str
    def __init__(self, index_version: _Optional[str] = ..., snapshot_version: _Optional[str] = ...) -> None: ...

class RunGateRequest(_message.Message):
    __slots__ = ("trigger", "candidate", "tenants", "doc_ids", "ruleset", "deadline", "idempotency_key")
    TRIGGER_FIELD_NUMBER: _ClassVar[int]
    CANDIDATE_FIELD_NUMBER: _ClassVar[int]
    TENANTS_FIELD_NUMBER: _ClassVar[int]
    DOC_IDS_FIELD_NUMBER: _ClassVar[int]
    RULESET_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    trigger: str
    candidate: EvalConfig
    tenants: _containers.RepeatedScalarFieldContainer[str]
    doc_ids: _containers.RepeatedScalarFieldContainer[str]
    ruleset: str
    deadline: _duration_pb2.Duration
    idempotency_key: str
    def __init__(self, trigger: _Optional[str] = ..., candidate: _Optional[_Union[EvalConfig, _Mapping]] = ..., tenants: _Optional[_Iterable[str]] = ..., doc_ids: _Optional[_Iterable[str]] = ..., ruleset: _Optional[str] = ..., deadline: _Optional[_Union[datetime.timedelta, _duration_pb2.Duration, _Mapping]] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class RunGateResponse(_message.Message):
    __slots__ = ("gate_id",)
    GATE_ID_FIELD_NUMBER: _ClassVar[int]
    gate_id: str
    def __init__(self, gate_id: _Optional[str] = ...) -> None: ...

class GetGateRequest(_message.Message):
    __slots__ = ("gate_id",)
    GATE_ID_FIELD_NUMBER: _ClassVar[int]
    gate_id: str
    def __init__(self, gate_id: _Optional[str] = ...) -> None: ...

class GetGateResponse(_message.Message):
    __slots__ = ("gate_id", "verdict", "report_uri", "violations")
    GATE_ID_FIELD_NUMBER: _ClassVar[int]
    VERDICT_FIELD_NUMBER: _ClassVar[int]
    REPORT_URI_FIELD_NUMBER: _ClassVar[int]
    VIOLATIONS_FIELD_NUMBER: _ClassVar[int]
    gate_id: str
    verdict: Verdict
    report_uri: str
    violations: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, gate_id: _Optional[str] = ..., verdict: _Optional[_Union[Verdict, str]] = ..., report_uri: _Optional[str] = ..., violations: _Optional[_Iterable[str]] = ...) -> None: ...
