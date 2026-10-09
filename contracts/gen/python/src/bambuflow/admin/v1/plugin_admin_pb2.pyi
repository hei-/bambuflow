import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PluginEvaluation(_message.Message):
    __slots__ = ("tenant", "passed", "report_ref", "evaluated_at")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    PASSED_FIELD_NUMBER: _ClassVar[int]
    REPORT_REF_FIELD_NUMBER: _ClassVar[int]
    EVALUATED_AT_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    passed: bool
    report_ref: str
    evaluated_at: _timestamp_pb2.Timestamp
    def __init__(self, tenant: _Optional[str] = ..., passed: _Optional[bool] = ..., report_ref: _Optional[str] = ..., evaluated_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class Plugin(_message.Message):
    __slots__ = ("plugin_id", "kind", "name", "version", "host", "description", "tool_scopes", "registered_by", "registered_at", "evaluations", "security_review", "reviewed_by", "reviewed_at", "review_note", "usable_tenants")
    PLUGIN_ID_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    HOST_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    TOOL_SCOPES_FIELD_NUMBER: _ClassVar[int]
    REGISTERED_BY_FIELD_NUMBER: _ClassVar[int]
    REGISTERED_AT_FIELD_NUMBER: _ClassVar[int]
    EVALUATIONS_FIELD_NUMBER: _ClassVar[int]
    SECURITY_REVIEW_FIELD_NUMBER: _ClassVar[int]
    REVIEWED_BY_FIELD_NUMBER: _ClassVar[int]
    REVIEWED_AT_FIELD_NUMBER: _ClassVar[int]
    REVIEW_NOTE_FIELD_NUMBER: _ClassVar[int]
    USABLE_TENANTS_FIELD_NUMBER: _ClassVar[int]
    plugin_id: str
    kind: str
    name: str
    version: str
    host: str
    description: str
    tool_scopes: _containers.RepeatedScalarFieldContainer[str]
    registered_by: str
    registered_at: _timestamp_pb2.Timestamp
    evaluations: _containers.RepeatedCompositeFieldContainer[PluginEvaluation]
    security_review: str
    reviewed_by: str
    reviewed_at: _timestamp_pb2.Timestamp
    review_note: str
    usable_tenants: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, plugin_id: _Optional[str] = ..., kind: _Optional[str] = ..., name: _Optional[str] = ..., version: _Optional[str] = ..., host: _Optional[str] = ..., description: _Optional[str] = ..., tool_scopes: _Optional[_Iterable[str]] = ..., registered_by: _Optional[str] = ..., registered_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., evaluations: _Optional[_Iterable[_Union[PluginEvaluation, _Mapping]]] = ..., security_review: _Optional[str] = ..., reviewed_by: _Optional[str] = ..., reviewed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., review_note: _Optional[str] = ..., usable_tenants: _Optional[_Iterable[str]] = ...) -> None: ...

class ListPluginsRequest(_message.Message):
    __slots__ = ("page_size", "page_token")
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    page_size: int
    page_token: str
    def __init__(self, page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListPluginsResponse(_message.Message):
    __slots__ = ("plugins", "next_page_token")
    PLUGINS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    plugins: _containers.RepeatedCompositeFieldContainer[Plugin]
    next_page_token: str
    def __init__(self, plugins: _Optional[_Iterable[_Union[Plugin, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class RegisterPluginRequest(_message.Message):
    __slots__ = ("kind", "name", "version", "description", "tool_scopes")
    KIND_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    TOOL_SCOPES_FIELD_NUMBER: _ClassVar[int]
    kind: str
    name: str
    version: str
    description: str
    tool_scopes: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, kind: _Optional[str] = ..., name: _Optional[str] = ..., version: _Optional[str] = ..., description: _Optional[str] = ..., tool_scopes: _Optional[_Iterable[str]] = ...) -> None: ...

class RegisterPluginResponse(_message.Message):
    __slots__ = ("plugin",)
    PLUGIN_FIELD_NUMBER: _ClassVar[int]
    plugin: Plugin
    def __init__(self, plugin: _Optional[_Union[Plugin, _Mapping]] = ...) -> None: ...

class ReviewPluginRequest(_message.Message):
    __slots__ = ("plugin_id", "approved", "note")
    PLUGIN_ID_FIELD_NUMBER: _ClassVar[int]
    APPROVED_FIELD_NUMBER: _ClassVar[int]
    NOTE_FIELD_NUMBER: _ClassVar[int]
    plugin_id: str
    approved: bool
    note: str
    def __init__(self, plugin_id: _Optional[str] = ..., approved: _Optional[bool] = ..., note: _Optional[str] = ...) -> None: ...

class ReviewPluginResponse(_message.Message):
    __slots__ = ("plugin",)
    PLUGIN_FIELD_NUMBER: _ClassVar[int]
    plugin: Plugin
    def __init__(self, plugin: _Optional[_Union[Plugin, _Mapping]] = ...) -> None: ...

class RecordEvaluationRequest(_message.Message):
    __slots__ = ("plugin_id", "tenant", "passed", "report_ref")
    PLUGIN_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    PASSED_FIELD_NUMBER: _ClassVar[int]
    REPORT_REF_FIELD_NUMBER: _ClassVar[int]
    plugin_id: str
    tenant: str
    passed: bool
    report_ref: str
    def __init__(self, plugin_id: _Optional[str] = ..., tenant: _Optional[str] = ..., passed: _Optional[bool] = ..., report_ref: _Optional[str] = ...) -> None: ...

class RecordEvaluationResponse(_message.Message):
    __slots__ = ("plugin",)
    PLUGIN_FIELD_NUMBER: _ClassVar[int]
    plugin: Plugin
    def __init__(self, plugin: _Optional[_Union[Plugin, _Mapping]] = ...) -> None: ...
