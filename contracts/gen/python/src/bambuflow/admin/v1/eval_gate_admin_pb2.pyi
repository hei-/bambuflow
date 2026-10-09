import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GateMetric(_message.Message):
    __slots__ = ("name", "value")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    name: str
    value: float
    def __init__(self, name: _Optional[str] = ..., value: _Optional[float] = ...) -> None: ...

class GateConfigVersion(_message.Message):
    __slots__ = ("name", "values")
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    name: str
    values: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, name: _Optional[str] = ..., values: _Optional[_Iterable[str]] = ...) -> None: ...

class Gate(_message.Message):
    __slots__ = ("gate_id", "trigger", "golden_set", "golden_version", "tenants", "conclusion", "reasons", "unstable", "metrics", "config", "repeats", "created_at")
    GATE_ID_FIELD_NUMBER: _ClassVar[int]
    TRIGGER_FIELD_NUMBER: _ClassVar[int]
    GOLDEN_SET_FIELD_NUMBER: _ClassVar[int]
    GOLDEN_VERSION_FIELD_NUMBER: _ClassVar[int]
    TENANTS_FIELD_NUMBER: _ClassVar[int]
    CONCLUSION_FIELD_NUMBER: _ClassVar[int]
    REASONS_FIELD_NUMBER: _ClassVar[int]
    UNSTABLE_FIELD_NUMBER: _ClassVar[int]
    METRICS_FIELD_NUMBER: _ClassVar[int]
    CONFIG_FIELD_NUMBER: _ClassVar[int]
    REPEATS_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    gate_id: str
    trigger: str
    golden_set: str
    golden_version: str
    tenants: _containers.RepeatedScalarFieldContainer[str]
    conclusion: str
    reasons: _containers.RepeatedScalarFieldContainer[str]
    unstable: _containers.RepeatedScalarFieldContainer[str]
    metrics: _containers.RepeatedCompositeFieldContainer[GateMetric]
    config: _containers.RepeatedCompositeFieldContainer[GateConfigVersion]
    repeats: int
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, gate_id: _Optional[str] = ..., trigger: _Optional[str] = ..., golden_set: _Optional[str] = ..., golden_version: _Optional[str] = ..., tenants: _Optional[_Iterable[str]] = ..., conclusion: _Optional[str] = ..., reasons: _Optional[_Iterable[str]] = ..., unstable: _Optional[_Iterable[str]] = ..., metrics: _Optional[_Iterable[_Union[GateMetric, _Mapping]]] = ..., config: _Optional[_Iterable[_Union[GateConfigVersion, _Mapping]]] = ..., repeats: _Optional[int] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ListGatesRequest(_message.Message):
    __slots__ = ("tenant", "trigger", "page_size", "page_token")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    TRIGGER_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    trigger: str
    page_size: int
    page_token: str
    def __init__(self, tenant: _Optional[str] = ..., trigger: _Optional[str] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListGatesResponse(_message.Message):
    __slots__ = ("gates", "next_page_token")
    GATES_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    gates: _containers.RepeatedCompositeFieldContainer[Gate]
    next_page_token: str
    def __init__(self, gates: _Optional[_Iterable[_Union[Gate, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class GetGateRequest(_message.Message):
    __slots__ = ("gate_id",)
    GATE_ID_FIELD_NUMBER: _ClassVar[int]
    gate_id: str
    def __init__(self, gate_id: _Optional[str] = ...) -> None: ...

class GetGateResponse(_message.Message):
    __slots__ = ("gate", "report_markdown")
    GATE_FIELD_NUMBER: _ClassVar[int]
    REPORT_MARKDOWN_FIELD_NUMBER: _ClassVar[int]
    gate: Gate
    report_markdown: str
    def __init__(self, gate: _Optional[_Union[Gate, _Mapping]] = ..., report_markdown: _Optional[str] = ...) -> None: ...
