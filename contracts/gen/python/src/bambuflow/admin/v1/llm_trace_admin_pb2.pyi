import datetime

from bambuflow.common.v1 import types_pb2 as _types_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GetLlmTraceRequest(_message.Message):
    __slots__ = ("trace_id",)
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    trace_id: str
    def __init__(self, trace_id: _Optional[str] = ...) -> None: ...

class LlmCall(_message.Message):
    __slots__ = ("use", "model", "level", "system_prompt", "user_prompt", "output", "prompt_tokens", "completion_tokens")
    USE_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    SYSTEM_PROMPT_FIELD_NUMBER: _ClassVar[int]
    USER_PROMPT_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_FIELD_NUMBER: _ClassVar[int]
    PROMPT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    COMPLETION_TOKENS_FIELD_NUMBER: _ClassVar[int]
    use: str
    model: str
    level: _types_pb2.Classification
    system_prompt: str
    user_prompt: str
    output: str
    prompt_tokens: int
    completion_tokens: int
    def __init__(self, use: _Optional[str] = ..., model: _Optional[str] = ..., level: _Optional[_Union[_types_pb2.Classification, str]] = ..., system_prompt: _Optional[str] = ..., user_prompt: _Optional[str] = ..., output: _Optional[str] = ..., prompt_tokens: _Optional[int] = ..., completion_tokens: _Optional[int] = ...) -> None: ...

class SentenceVerdict(_message.Message):
    __slots__ = ("text", "passed", "reason", "support", "refs")
    TEXT_FIELD_NUMBER: _ClassVar[int]
    PASSED_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    SUPPORT_FIELD_NUMBER: _ClassVar[int]
    REFS_FIELD_NUMBER: _ClassVar[int]
    text: str
    passed: bool
    reason: str
    support: float
    refs: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, text: _Optional[str] = ..., passed: _Optional[bool] = ..., reason: _Optional[str] = ..., support: _Optional[float] = ..., refs: _Optional[_Iterable[int]] = ...) -> None: ...

class LlmTrace(_message.Message):
    __slots__ = ("trace_id", "answer_id", "tenant", "created_at", "calls", "verdicts")
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    ANSWER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    CALLS_FIELD_NUMBER: _ClassVar[int]
    VERDICTS_FIELD_NUMBER: _ClassVar[int]
    trace_id: str
    answer_id: str
    tenant: str
    created_at: _timestamp_pb2.Timestamp
    calls: _containers.RepeatedCompositeFieldContainer[LlmCall]
    verdicts: _containers.RepeatedCompositeFieldContainer[SentenceVerdict]
    def __init__(self, trace_id: _Optional[str] = ..., answer_id: _Optional[str] = ..., tenant: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., calls: _Optional[_Iterable[_Union[LlmCall, _Mapping]]] = ..., verdicts: _Optional[_Iterable[_Union[SentenceVerdict, _Mapping]]] = ...) -> None: ...

class GetLlmTraceResponse(_message.Message):
    __slots__ = ("trace",)
    TRACE_FIELD_NUMBER: _ClassVar[int]
    trace: LlmTrace
    def __init__(self, trace: _Optional[_Union[LlmTrace, _Mapping]] = ...) -> None: ...
