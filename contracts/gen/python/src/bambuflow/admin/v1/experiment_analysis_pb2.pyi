import datetime

from bambuflow.admin.v1 import experiment_admin_pb2 as _experiment_admin_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GetExperimentAnalysisRequest(_message.Message):
    __slots__ = ("exp_id",)
    EXP_ID_FIELD_NUMBER: _ClassVar[int]
    exp_id: str
    def __init__(self, exp_id: _Optional[str] = ...) -> None: ...

class GetExperimentAnalysisResponse(_message.Message):
    __slots__ = ("analysis",)
    ANALYSIS_FIELD_NUMBER: _ClassVar[int]
    analysis: ExperimentAnalysis
    def __init__(self, analysis: _Optional[_Union[ExperimentAnalysis, _Mapping]] = ...) -> None: ...

class ArmSummary(_message.Message):
    __slots__ = ("arm", "users", "answers", "metric_users", "metric_mean", "refused_rate", "p95_latency_ms", "mean_tokens")
    ARM_FIELD_NUMBER: _ClassVar[int]
    USERS_FIELD_NUMBER: _ClassVar[int]
    ANSWERS_FIELD_NUMBER: _ClassVar[int]
    METRIC_USERS_FIELD_NUMBER: _ClassVar[int]
    METRIC_MEAN_FIELD_NUMBER: _ClassVar[int]
    REFUSED_RATE_FIELD_NUMBER: _ClassVar[int]
    P95_LATENCY_MS_FIELD_NUMBER: _ClassVar[int]
    MEAN_TOKENS_FIELD_NUMBER: _ClassVar[int]
    arm: str
    users: int
    answers: int
    metric_users: int
    metric_mean: float
    refused_rate: float
    p95_latency_ms: int
    mean_tokens: float
    def __init__(self, arm: _Optional[str] = ..., users: _Optional[int] = ..., answers: _Optional[int] = ..., metric_users: _Optional[int] = ..., metric_mean: _Optional[float] = ..., refused_rate: _Optional[float] = ..., p95_latency_ms: _Optional[int] = ..., mean_tokens: _Optional[float] = ...) -> None: ...

class GuardrailCheck(_message.Message):
    __slots__ = ("name", "control", "treatment", "breached")
    NAME_FIELD_NUMBER: _ClassVar[int]
    CONTROL_FIELD_NUMBER: _ClassVar[int]
    TREATMENT_FIELD_NUMBER: _ClassVar[int]
    BREACHED_FIELD_NUMBER: _ClassVar[int]
    name: str
    control: float
    treatment: float
    breached: bool
    def __init__(self, name: _Optional[str] = ..., control: _Optional[float] = ..., treatment: _Optional[float] = ..., breached: _Optional[bool] = ...) -> None: ...

class Conclusion(_message.Message):
    __slots__ = ("delta", "ci_low", "ci_high", "p_value", "significant", "improved")
    DELTA_FIELD_NUMBER: _ClassVar[int]
    CI_LOW_FIELD_NUMBER: _ClassVar[int]
    CI_HIGH_FIELD_NUMBER: _ClassVar[int]
    P_VALUE_FIELD_NUMBER: _ClassVar[int]
    SIGNIFICANT_FIELD_NUMBER: _ClassVar[int]
    IMPROVED_FIELD_NUMBER: _ClassVar[int]
    delta: float
    ci_low: float
    ci_high: float
    p_value: float
    significant: bool
    improved: bool
    def __init__(self, delta: _Optional[float] = ..., ci_low: _Optional[float] = ..., ci_high: _Optional[float] = ..., p_value: _Optional[float] = ..., significant: _Optional[bool] = ..., improved: _Optional[bool] = ...) -> None: ...

class ExperimentAnalysis(_message.Message):
    __slots__ = ("experiment", "control", "treatment", "progress", "guardrails", "conclusion", "data_since", "data_until")
    EXPERIMENT_FIELD_NUMBER: _ClassVar[int]
    CONTROL_FIELD_NUMBER: _ClassVar[int]
    TREATMENT_FIELD_NUMBER: _ClassVar[int]
    PROGRESS_FIELD_NUMBER: _ClassVar[int]
    GUARDRAILS_FIELD_NUMBER: _ClassVar[int]
    CONCLUSION_FIELD_NUMBER: _ClassVar[int]
    DATA_SINCE_FIELD_NUMBER: _ClassVar[int]
    DATA_UNTIL_FIELD_NUMBER: _ClassVar[int]
    experiment: _experiment_admin_pb2.Experiment
    control: ArmSummary
    treatment: ArmSummary
    progress: float
    guardrails: _containers.RepeatedCompositeFieldContainer[GuardrailCheck]
    conclusion: Conclusion
    data_since: _timestamp_pb2.Timestamp
    data_until: _timestamp_pb2.Timestamp
    def __init__(self, experiment: _Optional[_Union[_experiment_admin_pb2.Experiment, _Mapping]] = ..., control: _Optional[_Union[ArmSummary, _Mapping]] = ..., treatment: _Optional[_Union[ArmSummary, _Mapping]] = ..., progress: _Optional[float] = ..., guardrails: _Optional[_Iterable[_Union[GuardrailCheck, _Mapping]]] = ..., conclusion: _Optional[_Union[Conclusion, _Mapping]] = ..., data_since: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., data_until: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
