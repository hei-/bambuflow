import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ListSnapshotSyncRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class InstanceSync(_message.Message):
    __slots__ = ("instance_id", "snapshot_version", "loaded", "reason", "acked_at", "in_sync")
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_VERSION_FIELD_NUMBER: _ClassVar[int]
    LOADED_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    ACKED_AT_FIELD_NUMBER: _ClassVar[int]
    IN_SYNC_FIELD_NUMBER: _ClassVar[int]
    instance_id: str
    snapshot_version: str
    loaded: bool
    reason: str
    acked_at: _timestamp_pb2.Timestamp
    in_sync: bool
    def __init__(self, instance_id: _Optional[str] = ..., snapshot_version: _Optional[str] = ..., loaded: _Optional[bool] = ..., reason: _Optional[str] = ..., acked_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., in_sync: _Optional[bool] = ...) -> None: ...

class ServiceSync(_message.Message):
    __slots__ = ("service", "current_version", "instances")
    SERVICE_FIELD_NUMBER: _ClassVar[int]
    CURRENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    INSTANCES_FIELD_NUMBER: _ClassVar[int]
    service: str
    current_version: str
    instances: _containers.RepeatedCompositeFieldContainer[InstanceSync]
    def __init__(self, service: _Optional[str] = ..., current_version: _Optional[str] = ..., instances: _Optional[_Iterable[_Union[InstanceSync, _Mapping]]] = ...) -> None: ...

class ListSnapshotSyncResponse(_message.Message):
    __slots__ = ("services",)
    SERVICES_FIELD_NUMBER: _ClassVar[int]
    services: _containers.RepeatedCompositeFieldContainer[ServiceSync]
    def __init__(self, services: _Optional[_Iterable[_Union[ServiceSync, _Mapping]]] = ...) -> None: ...
