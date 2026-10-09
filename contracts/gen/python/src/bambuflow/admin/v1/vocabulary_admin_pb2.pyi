from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Term(_message.Message):
    __slots__ = ("vocabulary", "code", "name", "synonyms", "parent", "status", "replaced_by", "owner")
    VOCABULARY_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SYNONYMS_FIELD_NUMBER: _ClassVar[int]
    PARENT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    REPLACED_BY_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    vocabulary: str
    code: str
    name: str
    synonyms: _containers.RepeatedScalarFieldContainer[str]
    parent: str
    status: str
    replaced_by: str
    owner: str
    def __init__(self, vocabulary: _Optional[str] = ..., code: _Optional[str] = ..., name: _Optional[str] = ..., synonyms: _Optional[_Iterable[str]] = ..., parent: _Optional[str] = ..., status: _Optional[str] = ..., replaced_by: _Optional[str] = ..., owner: _Optional[str] = ...) -> None: ...

class ListTermsRequest(_message.Message):
    __slots__ = ("vocabulary",)
    VOCABULARY_FIELD_NUMBER: _ClassVar[int]
    vocabulary: str
    def __init__(self, vocabulary: _Optional[str] = ...) -> None: ...

class ListTermsResponse(_message.Message):
    __slots__ = ("terms", "snapshot_version")
    TERMS_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_VERSION_FIELD_NUMBER: _ClassVar[int]
    terms: _containers.RepeatedCompositeFieldContainer[Term]
    snapshot_version: str
    def __init__(self, terms: _Optional[_Iterable[_Union[Term, _Mapping]]] = ..., snapshot_version: _Optional[str] = ...) -> None: ...

class UpsertTermRequest(_message.Message):
    __slots__ = ("vocabulary", "code", "name", "synonyms", "parent", "owner")
    VOCABULARY_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SYNONYMS_FIELD_NUMBER: _ClassVar[int]
    PARENT_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    vocabulary: str
    code: str
    name: str
    synonyms: _containers.RepeatedScalarFieldContainer[str]
    parent: str
    owner: str
    def __init__(self, vocabulary: _Optional[str] = ..., code: _Optional[str] = ..., name: _Optional[str] = ..., synonyms: _Optional[_Iterable[str]] = ..., parent: _Optional[str] = ..., owner: _Optional[str] = ...) -> None: ...

class UpsertTermResponse(_message.Message):
    __slots__ = ("term", "created")
    TERM_FIELD_NUMBER: _ClassVar[int]
    CREATED_FIELD_NUMBER: _ClassVar[int]
    term: Term
    created: bool
    def __init__(self, term: _Optional[_Union[Term, _Mapping]] = ..., created: _Optional[bool] = ...) -> None: ...

class DeprecateTermRequest(_message.Message):
    __slots__ = ("vocabulary", "code", "replaced_by", "reason")
    VOCABULARY_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    REPLACED_BY_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    vocabulary: str
    code: str
    replaced_by: str
    reason: str
    def __init__(self, vocabulary: _Optional[str] = ..., code: _Optional[str] = ..., replaced_by: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class DeprecateTermResponse(_message.Message):
    __slots__ = ("term",)
    TERM_FIELD_NUMBER: _ClassVar[int]
    term: Term
    def __init__(self, term: _Optional[_Union[Term, _Mapping]] = ...) -> None: ...
