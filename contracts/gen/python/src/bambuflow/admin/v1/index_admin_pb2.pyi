from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TermRemap(_message.Message):
    __slots__ = ("vocabulary", "from_code", "to_code")
    VOCABULARY_FIELD_NUMBER: _ClassVar[int]
    FROM_CODE_FIELD_NUMBER: _ClassVar[int]
    TO_CODE_FIELD_NUMBER: _ClassVar[int]
    vocabulary: str
    from_code: str
    to_code: str
    def __init__(self, vocabulary: _Optional[str] = ..., from_code: _Optional[str] = ..., to_code: _Optional[str] = ...) -> None: ...

class RemapTermsRequest(_message.Message):
    __slots__ = ("remaps",)
    REMAPS_FIELD_NUMBER: _ClassVar[int]
    remaps: _containers.RepeatedCompositeFieldContainer[TermRemap]
    def __init__(self, remaps: _Optional[_Iterable[_Union[TermRemap, _Mapping]]] = ...) -> None: ...

class RemapTermsResponse(_message.Message):
    __slots__ = ("documents",)
    DOCUMENTS_FIELD_NUMBER: _ClassVar[int]
    documents: int
    def __init__(self, documents: _Optional[int] = ...) -> None: ...

class PurgeDeletedRequest(_message.Message):
    __slots__ = ("grace_hours",)
    GRACE_HOURS_FIELD_NUMBER: _ClassVar[int]
    grace_hours: int
    def __init__(self, grace_hours: _Optional[int] = ...) -> None: ...

class PurgeDeletedResponse(_message.Message):
    __slots__ = ("purged",)
    PURGED_FIELD_NUMBER: _ClassVar[int]
    purged: int
    def __init__(self, purged: _Optional[int] = ...) -> None: ...

class ExpireDocumentsRequest(_message.Message):
    __slots__ = ("retention_days",)
    class RetentionDaysEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    RETENTION_DAYS_FIELD_NUMBER: _ClassVar[int]
    retention_days: _containers.ScalarMap[str, int]
    def __init__(self, retention_days: _Optional[_Mapping[str, int]] = ...) -> None: ...

class ExpireDocumentsResponse(_message.Message):
    __slots__ = ("tombstoned",)
    class TombstonedEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    TOMBSTONED_FIELD_NUMBER: _ClassVar[int]
    tombstoned: _containers.ScalarMap[str, int]
    def __init__(self, tombstoned: _Optional[_Mapping[str, int]] = ...) -> None: ...

class AddDenyRulesRequest(_message.Message):
    __slots__ = ("doc_ids", "reason", "source_ids", "tenants")
    DOC_IDS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    SOURCE_IDS_FIELD_NUMBER: _ClassVar[int]
    TENANTS_FIELD_NUMBER: _ClassVar[int]
    doc_ids: _containers.RepeatedScalarFieldContainer[str]
    reason: str
    source_ids: _containers.RepeatedScalarFieldContainer[str]
    tenants: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, doc_ids: _Optional[_Iterable[str]] = ..., reason: _Optional[str] = ..., source_ids: _Optional[_Iterable[str]] = ..., tenants: _Optional[_Iterable[str]] = ...) -> None: ...

class AddDenyRulesResponse(_message.Message):
    __slots__ = ("added",)
    ADDED_FIELD_NUMBER: _ClassVar[int]
    added: int
    def __init__(self, added: _Optional[int] = ...) -> None: ...

class RemoveDenyRulesRequest(_message.Message):
    __slots__ = ("doc_ids",)
    DOC_IDS_FIELD_NUMBER: _ClassVar[int]
    doc_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, doc_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class RemoveDenyRulesResponse(_message.Message):
    __slots__ = ("removed",)
    REMOVED_FIELD_NUMBER: _ClassVar[int]
    removed: int
    def __init__(self, removed: _Optional[int] = ...) -> None: ...

class PublishRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class PublishResponse(_message.Message):
    __slots__ = ("version", "carried")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    CARRIED_FIELD_NUMBER: _ClassVar[int]
    version: str
    carried: int
    def __init__(self, version: _Optional[str] = ..., carried: _Optional[int] = ...) -> None: ...

class RollbackRequest(_message.Message):
    __slots__ = ("target_version",)
    TARGET_VERSION_FIELD_NUMBER: _ClassVar[int]
    target_version: str
    def __init__(self, target_version: _Optional[str] = ...) -> None: ...

class RollbackResponse(_message.Message):
    __slots__ = ("from_version", "to_version")
    FROM_VERSION_FIELD_NUMBER: _ClassVar[int]
    TO_VERSION_FIELD_NUMBER: _ClassVar[int]
    from_version: str
    to_version: str
    def __init__(self, from_version: _Optional[str] = ..., to_version: _Optional[str] = ...) -> None: ...
