from bambuflow.admin.v1 import experiment_admin_pb2 as _experiment_admin_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GetExperimentRequest(_message.Message):
    __slots__ = ("exp_id",)
    EXP_ID_FIELD_NUMBER: _ClassVar[int]
    exp_id: str
    def __init__(self, exp_id: _Optional[str] = ...) -> None: ...

class GetExperimentResponse(_message.Message):
    __slots__ = ("experiment",)
    EXPERIMENT_FIELD_NUMBER: _ClassVar[int]
    experiment: _experiment_admin_pb2.Experiment
    def __init__(self, experiment: _Optional[_Union[_experiment_admin_pb2.Experiment, _Mapping]] = ...) -> None: ...

class ListRunningExperimentsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListRunningExperimentsResponse(_message.Message):
    __slots__ = ("experiments",)
    EXPERIMENTS_FIELD_NUMBER: _ClassVar[int]
    experiments: _containers.RepeatedCompositeFieldContainer[_experiment_admin_pb2.Experiment]
    def __init__(self, experiments: _Optional[_Iterable[_Union[_experiment_admin_pb2.Experiment, _Mapping]]] = ...) -> None: ...

class StopForGuardrailRequest(_message.Message):
    __slots__ = ("exp_id", "reason")
    EXP_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    exp_id: str
    reason: str
    def __init__(self, exp_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class StopForGuardrailResponse(_message.Message):
    __slots__ = ("experiment",)
    EXPERIMENT_FIELD_NUMBER: _ClassVar[int]
    experiment: _experiment_admin_pb2.Experiment
    def __init__(self, experiment: _Optional[_Union[_experiment_admin_pb2.Experiment, _Mapping]] = ...) -> None: ...
