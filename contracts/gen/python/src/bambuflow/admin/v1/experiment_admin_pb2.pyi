import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PrimaryMetric(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PRIMARY_METRIC_UNSPECIFIED: _ClassVar[PrimaryMetric]
    PRIMARY_METRIC_USEFUL_RATE: _ClassVar[PrimaryMetric]
    PRIMARY_METRIC_INSUFFICIENT_EVIDENCE_RATE: _ClassVar[PrimaryMetric]

class ExperimentStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EXPERIMENT_STATUS_UNSPECIFIED: _ClassVar[ExperimentStatus]
    EXPERIMENT_STATUS_RUNNING: _ClassVar[ExperimentStatus]
    EXPERIMENT_STATUS_STOPPED: _ClassVar[ExperimentStatus]
PRIMARY_METRIC_UNSPECIFIED: PrimaryMetric
PRIMARY_METRIC_USEFUL_RATE: PrimaryMetric
PRIMARY_METRIC_INSUFFICIENT_EVIDENCE_RATE: PrimaryMetric
EXPERIMENT_STATUS_UNSPECIFIED: ExperimentStatus
EXPERIMENT_STATUS_RUNNING: ExperimentStatus
EXPERIMENT_STATUS_STOPPED: ExperimentStatus

class ExperimentOverrides(_message.Message):
    __slots__ = ("support_threshold", "min_coverage")
    SUPPORT_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    MIN_COVERAGE_FIELD_NUMBER: _ClassVar[int]
    support_threshold: float
    min_coverage: float
    def __init__(self, support_threshold: _Optional[float] = ..., min_coverage: _Optional[float] = ...) -> None: ...

class Experiment(_message.Message):
    __slots__ = ("exp_id", "tenant", "hypothesis", "primary_metric", "baseline_rate", "min_detectable_effect", "sample_size_per_arm", "traffic_percent", "overrides", "status", "created_by", "created_at", "stopped_by", "stopped_at", "stop_reason")
    EXP_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    HYPOTHESIS_FIELD_NUMBER: _ClassVar[int]
    PRIMARY_METRIC_FIELD_NUMBER: _ClassVar[int]
    BASELINE_RATE_FIELD_NUMBER: _ClassVar[int]
    MIN_DETECTABLE_EFFECT_FIELD_NUMBER: _ClassVar[int]
    SAMPLE_SIZE_PER_ARM_FIELD_NUMBER: _ClassVar[int]
    TRAFFIC_PERCENT_FIELD_NUMBER: _ClassVar[int]
    OVERRIDES_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CREATED_BY_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    STOPPED_BY_FIELD_NUMBER: _ClassVar[int]
    STOPPED_AT_FIELD_NUMBER: _ClassVar[int]
    STOP_REASON_FIELD_NUMBER: _ClassVar[int]
    exp_id: str
    tenant: str
    hypothesis: str
    primary_metric: PrimaryMetric
    baseline_rate: float
    min_detectable_effect: float
    sample_size_per_arm: int
    traffic_percent: int
    overrides: ExperimentOverrides
    status: ExperimentStatus
    created_by: str
    created_at: _timestamp_pb2.Timestamp
    stopped_by: str
    stopped_at: _timestamp_pb2.Timestamp
    stop_reason: str
    def __init__(self, exp_id: _Optional[str] = ..., tenant: _Optional[str] = ..., hypothesis: _Optional[str] = ..., primary_metric: _Optional[_Union[PrimaryMetric, str]] = ..., baseline_rate: _Optional[float] = ..., min_detectable_effect: _Optional[float] = ..., sample_size_per_arm: _Optional[int] = ..., traffic_percent: _Optional[int] = ..., overrides: _Optional[_Union[ExperimentOverrides, _Mapping]] = ..., status: _Optional[_Union[ExperimentStatus, str]] = ..., created_by: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., stopped_by: _Optional[str] = ..., stopped_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., stop_reason: _Optional[str] = ...) -> None: ...

class CreateExperimentRequest(_message.Message):
    __slots__ = ("tenant", "hypothesis", "primary_metric", "baseline_rate", "min_detectable_effect", "overrides")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    HYPOTHESIS_FIELD_NUMBER: _ClassVar[int]
    PRIMARY_METRIC_FIELD_NUMBER: _ClassVar[int]
    BASELINE_RATE_FIELD_NUMBER: _ClassVar[int]
    MIN_DETECTABLE_EFFECT_FIELD_NUMBER: _ClassVar[int]
    OVERRIDES_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    hypothesis: str
    primary_metric: PrimaryMetric
    baseline_rate: float
    min_detectable_effect: float
    overrides: ExperimentOverrides
    def __init__(self, tenant: _Optional[str] = ..., hypothesis: _Optional[str] = ..., primary_metric: _Optional[_Union[PrimaryMetric, str]] = ..., baseline_rate: _Optional[float] = ..., min_detectable_effect: _Optional[float] = ..., overrides: _Optional[_Union[ExperimentOverrides, _Mapping]] = ...) -> None: ...

class RampExperimentRequest(_message.Message):
    __slots__ = ("exp_id", "traffic_percent")
    EXP_ID_FIELD_NUMBER: _ClassVar[int]
    TRAFFIC_PERCENT_FIELD_NUMBER: _ClassVar[int]
    exp_id: str
    traffic_percent: int
    def __init__(self, exp_id: _Optional[str] = ..., traffic_percent: _Optional[int] = ...) -> None: ...

class StopExperimentRequest(_message.Message):
    __slots__ = ("exp_id", "reason")
    EXP_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    exp_id: str
    reason: str
    def __init__(self, exp_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class GetExperimentRequest(_message.Message):
    __slots__ = ("exp_id",)
    EXP_ID_FIELD_NUMBER: _ClassVar[int]
    exp_id: str
    def __init__(self, exp_id: _Optional[str] = ...) -> None: ...

class GetExperimentResponse(_message.Message):
    __slots__ = ("experiment",)
    EXPERIMENT_FIELD_NUMBER: _ClassVar[int]
    experiment: Experiment
    def __init__(self, experiment: _Optional[_Union[Experiment, _Mapping]] = ...) -> None: ...

class CreateExperimentResponse(_message.Message):
    __slots__ = ("experiment",)
    EXPERIMENT_FIELD_NUMBER: _ClassVar[int]
    experiment: Experiment
    def __init__(self, experiment: _Optional[_Union[Experiment, _Mapping]] = ...) -> None: ...

class RampExperimentResponse(_message.Message):
    __slots__ = ("experiment",)
    EXPERIMENT_FIELD_NUMBER: _ClassVar[int]
    experiment: Experiment
    def __init__(self, experiment: _Optional[_Union[Experiment, _Mapping]] = ...) -> None: ...

class StopExperimentResponse(_message.Message):
    __slots__ = ("experiment",)
    EXPERIMENT_FIELD_NUMBER: _ClassVar[int]
    experiment: Experiment
    def __init__(self, experiment: _Optional[_Union[Experiment, _Mapping]] = ...) -> None: ...

class ListExperimentsRequest(_message.Message):
    __slots__ = ("page_size", "page_token")
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    page_size: int
    page_token: str
    def __init__(self, page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListExperimentsResponse(_message.Message):
    __slots__ = ("experiments", "next_page_token")
    EXPERIMENTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    experiments: _containers.RepeatedCompositeFieldContainer[Experiment]
    next_page_token: str
    def __init__(self, experiments: _Optional[_Iterable[_Union[Experiment, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...
