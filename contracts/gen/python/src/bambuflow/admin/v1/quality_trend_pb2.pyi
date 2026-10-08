import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GetQualityTrendsRequest(_message.Message):
    __slots__ = ("tenant", "days")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    DAYS_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    days: int
    def __init__(self, tenant: _Optional[str] = ..., days: _Optional[int] = ...) -> None: ...

class GetQualityTrendsResponse(_message.Message):
    __slots__ = ("tenant", "days", "groundedness_7d", "groundedness_30d", "declining")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    DAYS_FIELD_NUMBER: _ClassVar[int]
    GROUNDEDNESS_7D_FIELD_NUMBER: _ClassVar[int]
    GROUNDEDNESS_30D_FIELD_NUMBER: _ClassVar[int]
    DECLINING_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    days: _containers.RepeatedCompositeFieldContainer[DailyQuality]
    groundedness_7d: float
    groundedness_30d: float
    declining: bool
    def __init__(self, tenant: _Optional[str] = ..., days: _Optional[_Iterable[_Union[DailyQuality, _Mapping]]] = ..., groundedness_7d: _Optional[float] = ..., groundedness_30d: _Optional[float] = ..., declining: _Optional[bool] = ...) -> None: ...

class DailyQuality(_message.Message):
    __slots__ = ("day", "sampled", "sentences", "groundedness", "citation_precision", "judge_model", "computed_at")
    DAY_FIELD_NUMBER: _ClassVar[int]
    SAMPLED_FIELD_NUMBER: _ClassVar[int]
    SENTENCES_FIELD_NUMBER: _ClassVar[int]
    GROUNDEDNESS_FIELD_NUMBER: _ClassVar[int]
    CITATION_PRECISION_FIELD_NUMBER: _ClassVar[int]
    JUDGE_MODEL_FIELD_NUMBER: _ClassVar[int]
    COMPUTED_AT_FIELD_NUMBER: _ClassVar[int]
    day: str
    sampled: int
    sentences: int
    groundedness: float
    citation_precision: float
    judge_model: str
    computed_at: _timestamp_pb2.Timestamp
    def __init__(self, day: _Optional[str] = ..., sampled: _Optional[int] = ..., sentences: _Optional[int] = ..., groundedness: _Optional[float] = ..., citation_precision: _Optional[float] = ..., judge_model: _Optional[str] = ..., computed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
