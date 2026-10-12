import datetime

from bambuflow.common.v1 import types_pb2 as _types_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AclResult(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ACL_RESULT_UNSPECIFIED: _ClassVar[AclResult]
    ACL_RESULT_UPDATED: _ClassVar[AclResult]
    ACL_RESULT_UNCHANGED: _ClassVar[AclResult]
    ACL_RESULT_NOT_FOUND: _ClassVar[AclResult]
    ACL_RESULT_STALE: _ClassVar[AclResult]

class TombstoneResult(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TOMBSTONE_RESULT_UNSPECIFIED: _ClassVar[TombstoneResult]
    TOMBSTONE_RESULT_TOMBSTONED: _ClassVar[TombstoneResult]
    TOMBSTONE_RESULT_UNCHANGED: _ClassVar[TombstoneResult]
    TOMBSTONE_RESULT_NOT_FOUND: _ClassVar[TombstoneResult]
    TOMBSTONE_RESULT_STALE: _ClassVar[TombstoneResult]

class WriteResult(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    WRITE_RESULT_UNSPECIFIED: _ClassVar[WriteResult]
    WRITE_RESULT_WRITTEN: _ClassVar[WriteResult]
    WRITE_RESULT_UNCHANGED: _ClassVar[WriteResult]
    WRITE_RESULT_STALE_VERSION: _ClassVar[WriteResult]
    WRITE_RESULT_MODEL_MISMATCH: _ClassVar[WriteResult]
ACL_RESULT_UNSPECIFIED: AclResult
ACL_RESULT_UPDATED: AclResult
ACL_RESULT_UNCHANGED: AclResult
ACL_RESULT_NOT_FOUND: AclResult
ACL_RESULT_STALE: AclResult
TOMBSTONE_RESULT_UNSPECIFIED: TombstoneResult
TOMBSTONE_RESULT_TOMBSTONED: TombstoneResult
TOMBSTONE_RESULT_UNCHANGED: TombstoneResult
TOMBSTONE_RESULT_NOT_FOUND: TombstoneResult
TOMBSTONE_RESULT_STALE: TombstoneResult
WRITE_RESULT_UNSPECIFIED: WriteResult
WRITE_RESULT_WRITTEN: WriteResult
WRITE_RESULT_UNCHANGED: WriteResult
WRITE_RESULT_STALE_VERSION: WriteResult
WRITE_RESULT_MODEL_MISMATCH: WriteResult

class RecordPurgeRequest(_message.Message):
    __slots__ = ("receipt_id", "store", "completed_at")
    RECEIPT_ID_FIELD_NUMBER: _ClassVar[int]
    STORE_FIELD_NUMBER: _ClassVar[int]
    COMPLETED_AT_FIELD_NUMBER: _ClassVar[int]
    receipt_id: str
    store: str
    completed_at: _timestamp_pb2.Timestamp
    def __init__(self, receipt_id: _Optional[str] = ..., store: _Optional[str] = ..., completed_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class RecordPurgeResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListSourceDocumentsRequest(_message.Message):
    __slots__ = ("source_id",)
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    def __init__(self, source_id: _Optional[str] = ...) -> None: ...

class IndexedDocument(_message.Message):
    __slots__ = ("doc_id", "content_hash", "acl_tokens", "classification", "tombstoned")
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    ACL_TOKENS_FIELD_NUMBER: _ClassVar[int]
    CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
    TOMBSTONED_FIELD_NUMBER: _ClassVar[int]
    doc_id: str
    content_hash: str
    acl_tokens: _containers.RepeatedScalarFieldContainer[str]
    classification: _types_pb2.Classification
    tombstoned: bool
    def __init__(self, doc_id: _Optional[str] = ..., content_hash: _Optional[str] = ..., acl_tokens: _Optional[_Iterable[str]] = ..., classification: _Optional[_Union[_types_pb2.Classification, str]] = ..., tombstoned: _Optional[bool] = ...) -> None: ...

class ListSourceDocumentsResponse(_message.Message):
    __slots__ = ("documents",)
    DOCUMENTS_FIELD_NUMBER: _ClassVar[int]
    documents: _containers.RepeatedCompositeFieldContainer[IndexedDocument]
    def __init__(self, documents: _Optional[_Iterable[_Union[IndexedDocument, _Mapping]]] = ...) -> None: ...

class UpdateAclRequest(_message.Message):
    __slots__ = ("source_id", "doc_id", "acl_tokens", "classification", "occurred_at")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    ACL_TOKENS_FIELD_NUMBER: _ClassVar[int]
    CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    doc_id: str
    acl_tokens: _containers.RepeatedScalarFieldContainer[str]
    classification: _types_pb2.Classification
    occurred_at: _timestamp_pb2.Timestamp
    def __init__(self, source_id: _Optional[str] = ..., doc_id: _Optional[str] = ..., acl_tokens: _Optional[_Iterable[str]] = ..., classification: _Optional[_Union[_types_pb2.Classification, str]] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class UpdateAclResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: AclResult
    def __init__(self, result: _Optional[_Union[AclResult, str]] = ...) -> None: ...

class LookupVectorsRequest(_message.Message):
    __slots__ = ("embedding_model_version", "input_hashes")
    EMBEDDING_MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    INPUT_HASHES_FIELD_NUMBER: _ClassVar[int]
    embedding_model_version: str
    input_hashes: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, embedding_model_version: _Optional[str] = ..., input_hashes: _Optional[_Iterable[str]] = ...) -> None: ...

class CachedVector(_message.Message):
    __slots__ = ("input_hash", "embedding", "sparse")
    class SparseEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: float
        def __init__(self, key: _Optional[int] = ..., value: _Optional[float] = ...) -> None: ...
    INPUT_HASH_FIELD_NUMBER: _ClassVar[int]
    EMBEDDING_FIELD_NUMBER: _ClassVar[int]
    SPARSE_FIELD_NUMBER: _ClassVar[int]
    input_hash: str
    embedding: _containers.RepeatedScalarFieldContainer[float]
    sparse: _containers.ScalarMap[int, float]
    def __init__(self, input_hash: _Optional[str] = ..., embedding: _Optional[_Iterable[float]] = ..., sparse: _Optional[_Mapping[int, float]] = ...) -> None: ...

class LookupVectorsResponse(_message.Message):
    __slots__ = ("vectors",)
    VECTORS_FIELD_NUMBER: _ClassVar[int]
    vectors: _containers.RepeatedCompositeFieldContainer[CachedVector]
    def __init__(self, vectors: _Optional[_Iterable[_Union[CachedVector, _Mapping]]] = ...) -> None: ...

class SetPartitionGrantsRequest(_message.Message):
    __slots__ = ("source_id", "partition", "grantees", "occurred_at")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    PARTITION_FIELD_NUMBER: _ClassVar[int]
    GRANTEES_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    partition: str
    grantees: _containers.RepeatedScalarFieldContainer[str]
    occurred_at: _timestamp_pb2.Timestamp
    def __init__(self, source_id: _Optional[str] = ..., partition: _Optional[str] = ..., grantees: _Optional[_Iterable[str]] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class SetPartitionGrantsResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: AclResult
    def __init__(self, result: _Optional[_Union[AclResult, str]] = ...) -> None: ...

class TombstoneDocumentRequest(_message.Message):
    __slots__ = ("source_id", "doc_id", "occurred_at")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    doc_id: str
    occurred_at: _timestamp_pb2.Timestamp
    def __init__(self, source_id: _Optional[str] = ..., doc_id: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class TombstoneDocumentResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: TombstoneResult
    def __init__(self, result: _Optional[_Union[TombstoneResult, str]] = ...) -> None: ...

class WriteDocumentRequest(_message.Message):
    __slots__ = ("idempotency_key", "source_id", "doc_id", "content_hash", "occurred_at", "title", "source_uri", "source_type", "owner", "classification", "acl_tokens", "chunks", "embedding_model_version", "chunker_version", "pipeline_version", "metadata", "relations", "enrichment")
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    CONTENT_HASH_FIELD_NUMBER: _ClassVar[int]
    OCCURRED_AT_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_URI_FIELD_NUMBER: _ClassVar[int]
    SOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
    ACL_TOKENS_FIELD_NUMBER: _ClassVar[int]
    CHUNKS_FIELD_NUMBER: _ClassVar[int]
    EMBEDDING_MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    CHUNKER_VERSION_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_VERSION_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    RELATIONS_FIELD_NUMBER: _ClassVar[int]
    ENRICHMENT_FIELD_NUMBER: _ClassVar[int]
    idempotency_key: str
    source_id: str
    doc_id: str
    content_hash: str
    occurred_at: _timestamp_pb2.Timestamp
    title: str
    source_uri: str
    source_type: _types_pb2.SourceType
    owner: str
    classification: _types_pb2.Classification
    acl_tokens: _containers.RepeatedScalarFieldContainer[str]
    chunks: _containers.RepeatedCompositeFieldContainer[Chunk]
    embedding_model_version: str
    chunker_version: str
    pipeline_version: str
    metadata: _types_pb2.DocMetadata
    relations: _containers.RepeatedCompositeFieldContainer[Relation]
    enrichment: Enrichment
    def __init__(self, idempotency_key: _Optional[str] = ..., source_id: _Optional[str] = ..., doc_id: _Optional[str] = ..., content_hash: _Optional[str] = ..., occurred_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., title: _Optional[str] = ..., source_uri: _Optional[str] = ..., source_type: _Optional[_Union[_types_pb2.SourceType, str]] = ..., owner: _Optional[str] = ..., classification: _Optional[_Union[_types_pb2.Classification, str]] = ..., acl_tokens: _Optional[_Iterable[str]] = ..., chunks: _Optional[_Iterable[_Union[Chunk, _Mapping]]] = ..., embedding_model_version: _Optional[str] = ..., chunker_version: _Optional[str] = ..., pipeline_version: _Optional[str] = ..., metadata: _Optional[_Union[_types_pb2.DocMetadata, _Mapping]] = ..., relations: _Optional[_Iterable[_Union[Relation, _Mapping]]] = ..., enrichment: _Optional[_Union[Enrichment, _Mapping]] = ...) -> None: ...

class Enrichment(_message.Message):
    __slots__ = ("summary_chunk", "keywords", "sections", "summarizer_version")
    SUMMARY_CHUNK_FIELD_NUMBER: _ClassVar[int]
    KEYWORDS_FIELD_NUMBER: _ClassVar[int]
    SECTIONS_FIELD_NUMBER: _ClassVar[int]
    SUMMARIZER_VERSION_FIELD_NUMBER: _ClassVar[int]
    summary_chunk: Chunk
    keywords: _containers.RepeatedScalarFieldContainer[str]
    sections: _containers.RepeatedCompositeFieldContainer[SectionSummary]
    summarizer_version: str
    def __init__(self, summary_chunk: _Optional[_Union[Chunk, _Mapping]] = ..., keywords: _Optional[_Iterable[str]] = ..., sections: _Optional[_Iterable[_Union[SectionSummary, _Mapping]]] = ..., summarizer_version: _Optional[str] = ...) -> None: ...

class SectionSummary(_message.Message):
    __slots__ = ("parent_chunk_id", "heading_path", "summary", "keywords")
    PARENT_CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    HEADING_PATH_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    KEYWORDS_FIELD_NUMBER: _ClassVar[int]
    parent_chunk_id: str
    heading_path: _containers.RepeatedScalarFieldContainer[str]
    summary: str
    keywords: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, parent_chunk_id: _Optional[str] = ..., heading_path: _Optional[_Iterable[str]] = ..., summary: _Optional[str] = ..., keywords: _Optional[_Iterable[str]] = ...) -> None: ...

class Entity(_message.Message):
    __slots__ = ("type", "name", "aliases")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ALIASES_FIELD_NUMBER: _ClassVar[int]
    type: _types_pb2.EntityType
    name: str
    aliases: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, type: _Optional[_Union[_types_pb2.EntityType, str]] = ..., name: _Optional[str] = ..., aliases: _Optional[_Iterable[str]] = ...) -> None: ...

class Relation(_message.Message):
    __slots__ = ("subject", "relation_type", "object", "evidence_chunk_id", "confidence", "extractor_version")
    SUBJECT_FIELD_NUMBER: _ClassVar[int]
    RELATION_TYPE_FIELD_NUMBER: _ClassVar[int]
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    EVIDENCE_CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    CONFIDENCE_FIELD_NUMBER: _ClassVar[int]
    EXTRACTOR_VERSION_FIELD_NUMBER: _ClassVar[int]
    subject: Entity
    relation_type: _types_pb2.RelationType
    object: Entity
    evidence_chunk_id: str
    confidence: float
    extractor_version: str
    def __init__(self, subject: _Optional[_Union[Entity, _Mapping]] = ..., relation_type: _Optional[_Union[_types_pb2.RelationType, str]] = ..., object: _Optional[_Union[Entity, _Mapping]] = ..., evidence_chunk_id: _Optional[str] = ..., confidence: _Optional[float] = ..., extractor_version: _Optional[str] = ...) -> None: ...

class Chunk(_message.Message):
    __slots__ = ("chunk_id", "parent_chunk_id", "heading_path", "text", "text_hash", "span", "embedding", "injection_flag", "pii", "input_hash", "sparse")
    class SparseEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: int
        value: float
        def __init__(self, key: _Optional[int] = ..., value: _Optional[float] = ...) -> None: ...
    CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    PARENT_CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    HEADING_PATH_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    TEXT_HASH_FIELD_NUMBER: _ClassVar[int]
    SPAN_FIELD_NUMBER: _ClassVar[int]
    EMBEDDING_FIELD_NUMBER: _ClassVar[int]
    INJECTION_FLAG_FIELD_NUMBER: _ClassVar[int]
    PII_FIELD_NUMBER: _ClassVar[int]
    INPUT_HASH_FIELD_NUMBER: _ClassVar[int]
    SPARSE_FIELD_NUMBER: _ClassVar[int]
    chunk_id: str
    parent_chunk_id: str
    heading_path: _containers.RepeatedScalarFieldContainer[str]
    text: str
    text_hash: str
    span: _types_pb2.Span
    embedding: _containers.RepeatedScalarFieldContainer[float]
    injection_flag: bool
    pii: SealedPII
    input_hash: str
    sparse: _containers.ScalarMap[int, float]
    def __init__(self, chunk_id: _Optional[str] = ..., parent_chunk_id: _Optional[str] = ..., heading_path: _Optional[_Iterable[str]] = ..., text: _Optional[str] = ..., text_hash: _Optional[str] = ..., span: _Optional[_Union[_types_pb2.Span, _Mapping]] = ..., embedding: _Optional[_Iterable[float]] = ..., injection_flag: _Optional[bool] = ..., pii: _Optional[_Union[SealedPII, _Mapping]] = ..., input_hash: _Optional[str] = ..., sparse: _Optional[_Mapping[int, float]] = ...) -> None: ...

class SealedPII(_message.Message):
    __slots__ = ("ciphertext", "wrapped_key", "key_id")
    CIPHERTEXT_FIELD_NUMBER: _ClassVar[int]
    WRAPPED_KEY_FIELD_NUMBER: _ClassVar[int]
    KEY_ID_FIELD_NUMBER: _ClassVar[int]
    ciphertext: bytes
    wrapped_key: str
    key_id: str
    def __init__(self, ciphertext: _Optional[bytes] = ..., wrapped_key: _Optional[str] = ..., key_id: _Optional[str] = ...) -> None: ...

class WriteDocumentResponse(_message.Message):
    __slots__ = ("result", "doc_version", "written_chunks", "index_version")
    RESULT_FIELD_NUMBER: _ClassVar[int]
    DOC_VERSION_FIELD_NUMBER: _ClassVar[int]
    WRITTEN_CHUNKS_FIELD_NUMBER: _ClassVar[int]
    INDEX_VERSION_FIELD_NUMBER: _ClassVar[int]
    result: WriteResult
    doc_version: int
    written_chunks: int
    index_version: str
    def __init__(self, result: _Optional[_Union[WriteResult, str]] = ..., doc_version: _Optional[int] = ..., written_chunks: _Optional[int] = ..., index_version: _Optional[str] = ...) -> None: ...
