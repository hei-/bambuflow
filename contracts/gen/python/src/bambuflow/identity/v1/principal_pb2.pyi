from bambuflow.common.v1 import types_pb2 as _types_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Principal(_message.Message):
    __slots__ = ("sub", "tenant", "dept", "title", "clearance", "region", "groups", "groups_ref", "actor", "auth_time", "iat", "exp", "jti", "aud", "roles")
    SUB_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    DEPT_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    CLEARANCE_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    GROUPS_FIELD_NUMBER: _ClassVar[int]
    GROUPS_REF_FIELD_NUMBER: _ClassVar[int]
    ACTOR_FIELD_NUMBER: _ClassVar[int]
    AUTH_TIME_FIELD_NUMBER: _ClassVar[int]
    IAT_FIELD_NUMBER: _ClassVar[int]
    EXP_FIELD_NUMBER: _ClassVar[int]
    JTI_FIELD_NUMBER: _ClassVar[int]
    AUD_FIELD_NUMBER: _ClassVar[int]
    ROLES_FIELD_NUMBER: _ClassVar[int]
    sub: str
    tenant: str
    dept: str
    title: str
    clearance: _types_pb2.Classification
    region: str
    groups: _containers.RepeatedScalarFieldContainer[str]
    groups_ref: str
    actor: str
    auth_time: int
    iat: int
    exp: int
    jti: str
    aud: str
    roles: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, sub: _Optional[str] = ..., tenant: _Optional[str] = ..., dept: _Optional[str] = ..., title: _Optional[str] = ..., clearance: _Optional[_Union[_types_pb2.Classification, str]] = ..., region: _Optional[str] = ..., groups: _Optional[_Iterable[str]] = ..., groups_ref: _Optional[str] = ..., actor: _Optional[str] = ..., auth_time: _Optional[int] = ..., iat: _Optional[int] = ..., exp: _Optional[int] = ..., jti: _Optional[str] = ..., aud: _Optional[str] = ..., roles: _Optional[_Iterable[str]] = ...) -> None: ...
