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

class SearchIntent(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SEARCH_INTENT_UNSPECIFIED: _ClassVar[SearchIntent]
    SEARCH_INTENT_RELATION: _ClassVar[SearchIntent]

class DocumentOrder(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DOCUMENT_ORDER_UNSPECIFIED: _ClassVar[DocumentOrder]
    DOCUMENT_ORDER_EFFECTIVE_DATE_DESC: _ClassVar[DocumentOrder]

class CountGroupBy(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    COUNT_GROUP_BY_UNSPECIFIED: _ClassVar[CountGroupBy]
    COUNT_GROUP_BY_DOC_TYPE: _ClassVar[CountGroupBy]
    COUNT_GROUP_BY_DEPT: _ClassVar[CountGroupBy]
    COUNT_GROUP_BY_VALIDITY_STATUS: _ClassVar[CountGroupBy]
    COUNT_GROUP_BY_YEAR: _ClassVar[CountGroupBy]

class AccessReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ACCESS_REASON_UNSPECIFIED: _ClassVar[AccessReason]
    ACCESS_REASON_ALLOWED: _ClassVar[AccessReason]
    ACCESS_REASON_ACL_DENIED: _ClassVar[AccessReason]
    ACCESS_REASON_CLASSIFICATION_EXCEEDED: _ClassVar[AccessReason]
    ACCESS_REASON_TOMBSTONED: _ClassVar[AccessReason]
    ACCESS_REASON_NOT_FOUND: _ClassVar[AccessReason]
SEARCH_INTENT_UNSPECIFIED: SearchIntent
SEARCH_INTENT_RELATION: SearchIntent
DOCUMENT_ORDER_UNSPECIFIED: DocumentOrder
DOCUMENT_ORDER_EFFECTIVE_DATE_DESC: DocumentOrder
COUNT_GROUP_BY_UNSPECIFIED: CountGroupBy
COUNT_GROUP_BY_DOC_TYPE: CountGroupBy
COUNT_GROUP_BY_DEPT: CountGroupBy
COUNT_GROUP_BY_VALIDITY_STATUS: CountGroupBy
COUNT_GROUP_BY_YEAR: CountGroupBy
ACCESS_REASON_UNSPECIFIED: AccessReason
ACCESS_REASON_ALLOWED: AccessReason
ACCESS_REASON_ACL_DENIED: AccessReason
ACCESS_REASON_CLASSIFICATION_EXCEEDED: AccessReason
ACCESS_REASON_TOMBSTONED: AccessReason
ACCESS_REASON_NOT_FOUND: AccessReason

class SubQuery(_message.Message):
    __slots__ = ("id", "text")
    ID_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    id: str
    text: str
    def __init__(self, id: _Optional[str] = ..., text: _Optional[str] = ...) -> None: ...

class SearchRequest(_message.Message):
    __slots__ = ("queries", "filters", "top_k", "rerank", "intent")
    QUERIES_FIELD_NUMBER: _ClassVar[int]
    FILTERS_FIELD_NUMBER: _ClassVar[int]
    TOP_K_FIELD_NUMBER: _ClassVar[int]
    RERANK_FIELD_NUMBER: _ClassVar[int]
    INTENT_FIELD_NUMBER: _ClassVar[int]
    queries: _containers.RepeatedCompositeFieldContainer[SubQuery]
    filters: _types_pb2.Filters
    top_k: int
    rerank: bool
    intent: SearchIntent
    def __init__(self, queries: _Optional[_Iterable[_Union[SubQuery, _Mapping]]] = ..., filters: _Optional[_Union[_types_pb2.Filters, _Mapping]] = ..., top_k: _Optional[int] = ..., rerank: _Optional[bool] = ..., intent: _Optional[_Union[SearchIntent, str]] = ...) -> None: ...

class SearchResponse(_message.Message):
    __slots__ = ("hits", "index_version", "policy_version", "degraded", "embedding_model", "rerank_model")
    HITS_FIELD_NUMBER: _ClassVar[int]
    INDEX_VERSION_FIELD_NUMBER: _ClassVar[int]
    POLICY_VERSION_FIELD_NUMBER: _ClassVar[int]
    DEGRADED_FIELD_NUMBER: _ClassVar[int]
    EMBEDDING_MODEL_FIELD_NUMBER: _ClassVar[int]
    RERANK_MODEL_FIELD_NUMBER: _ClassVar[int]
    hits: _containers.RepeatedCompositeFieldContainer[Hit]
    index_version: str
    policy_version: str
    degraded: _containers.RepeatedCompositeFieldContainer[_types_pb2.Degradation]
    embedding_model: str
    rerank_model: str
    def __init__(self, hits: _Optional[_Iterable[_Union[Hit, _Mapping]]] = ..., index_version: _Optional[str] = ..., policy_version: _Optional[str] = ..., degraded: _Optional[_Iterable[_Union[_types_pb2.Degradation, _Mapping]]] = ..., embedding_model: _Optional[str] = ..., rerank_model: _Optional[str] = ...) -> None: ...

class Hit(_message.Message):
    __slots__ = ("chunk_id", "doc_id", "doc_version", "parent_chunk_id", "text", "span", "title", "heading_path", "source_uri", "source_type", "owner", "classification", "score", "graph_path", "injection_flag")
    CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    DOC_VERSION_FIELD_NUMBER: _ClassVar[int]
    PARENT_CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    SPAN_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    HEADING_PATH_FIELD_NUMBER: _ClassVar[int]
    SOURCE_URI_FIELD_NUMBER: _ClassVar[int]
    SOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    GRAPH_PATH_FIELD_NUMBER: _ClassVar[int]
    INJECTION_FLAG_FIELD_NUMBER: _ClassVar[int]
    chunk_id: str
    doc_id: str
    doc_version: int
    parent_chunk_id: str
    text: str
    span: _types_pb2.Span
    title: str
    heading_path: _containers.RepeatedScalarFieldContainer[str]
    source_uri: str
    source_type: _types_pb2.SourceType
    owner: str
    classification: _types_pb2.Classification
    score: float
    graph_path: _containers.RepeatedCompositeFieldContainer[GraphEdge]
    injection_flag: bool
    def __init__(self, chunk_id: _Optional[str] = ..., doc_id: _Optional[str] = ..., doc_version: _Optional[int] = ..., parent_chunk_id: _Optional[str] = ..., text: _Optional[str] = ..., span: _Optional[_Union[_types_pb2.Span, _Mapping]] = ..., title: _Optional[str] = ..., heading_path: _Optional[_Iterable[str]] = ..., source_uri: _Optional[str] = ..., source_type: _Optional[_Union[_types_pb2.SourceType, str]] = ..., owner: _Optional[str] = ..., classification: _Optional[_Union[_types_pb2.Classification, str]] = ..., score: _Optional[float] = ..., graph_path: _Optional[_Iterable[_Union[GraphEdge, _Mapping]]] = ..., injection_flag: _Optional[bool] = ...) -> None: ...

class GraphEdge(_message.Message):
    __slots__ = ("from_entity", "from_type", "relation_type", "to_entity", "to_type")
    FROM_ENTITY_FIELD_NUMBER: _ClassVar[int]
    FROM_TYPE_FIELD_NUMBER: _ClassVar[int]
    RELATION_TYPE_FIELD_NUMBER: _ClassVar[int]
    TO_ENTITY_FIELD_NUMBER: _ClassVar[int]
    TO_TYPE_FIELD_NUMBER: _ClassVar[int]
    from_entity: str
    from_type: _types_pb2.EntityType
    relation_type: _types_pb2.RelationType
    to_entity: str
    to_type: _types_pb2.EntityType
    def __init__(self, from_entity: _Optional[str] = ..., from_type: _Optional[_Union[_types_pb2.EntityType, str]] = ..., relation_type: _Optional[_Union[_types_pb2.RelationType, str]] = ..., to_entity: _Optional[str] = ..., to_type: _Optional[_Union[_types_pb2.EntityType, str]] = ...) -> None: ...

class ListDocumentsRequest(_message.Message):
    __slots__ = ("filters", "page_size", "page_token", "order_by")
    FILTERS_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    ORDER_BY_FIELD_NUMBER: _ClassVar[int]
    filters: _types_pb2.Filters
    page_size: int
    page_token: str
    order_by: DocumentOrder
    def __init__(self, filters: _Optional[_Union[_types_pb2.Filters, _Mapping]] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ..., order_by: _Optional[_Union[DocumentOrder, str]] = ...) -> None: ...

class ListDocumentsResponse(_message.Message):
    __slots__ = ("documents", "total_count", "total_is_exact", "next_page_token", "index_version", "policy_version", "metadata_gaps")
    DOCUMENTS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    TOTAL_IS_EXACT_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    INDEX_VERSION_FIELD_NUMBER: _ClassVar[int]
    POLICY_VERSION_FIELD_NUMBER: _ClassVar[int]
    METADATA_GAPS_FIELD_NUMBER: _ClassVar[int]
    documents: _containers.RepeatedCompositeFieldContainer[DocumentSummary]
    total_count: int
    total_is_exact: bool
    next_page_token: str
    index_version: str
    policy_version: str
    metadata_gaps: _containers.RepeatedCompositeFieldContainer[MetadataGap]
    def __init__(self, documents: _Optional[_Iterable[_Union[DocumentSummary, _Mapping]]] = ..., total_count: _Optional[int] = ..., total_is_exact: _Optional[bool] = ..., next_page_token: _Optional[str] = ..., index_version: _Optional[str] = ..., policy_version: _Optional[str] = ..., metadata_gaps: _Optional[_Iterable[_Union[MetadataGap, _Mapping]]] = ...) -> None: ...

class MetadataGap(_message.Message):
    __slots__ = ("field", "count")
    FIELD_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    field: str
    count: int
    def __init__(self, field: _Optional[str] = ..., count: _Optional[int] = ...) -> None: ...

class DocumentSummary(_message.Message):
    __slots__ = ("doc_id", "doc_version", "title", "source_id", "source_uri", "source_type", "classification", "metadata", "summary_chunk_id")
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    DOC_VERSION_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_URI_FIELD_NUMBER: _ClassVar[int]
    SOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    doc_id: str
    doc_version: int
    title: str
    source_id: str
    source_uri: str
    source_type: _types_pb2.SourceType
    classification: _types_pb2.Classification
    metadata: _types_pb2.DocMetadata
    summary_chunk_id: str
    def __init__(self, doc_id: _Optional[str] = ..., doc_version: _Optional[int] = ..., title: _Optional[str] = ..., source_id: _Optional[str] = ..., source_uri: _Optional[str] = ..., source_type: _Optional[_Union[_types_pb2.SourceType, str]] = ..., classification: _Optional[_Union[_types_pb2.Classification, str]] = ..., metadata: _Optional[_Union[_types_pb2.DocMetadata, _Mapping]] = ..., summary_chunk_id: _Optional[str] = ...) -> None: ...

class CountDocumentsRequest(_message.Message):
    __slots__ = ("filters", "group_by")
    FILTERS_FIELD_NUMBER: _ClassVar[int]
    GROUP_BY_FIELD_NUMBER: _ClassVar[int]
    filters: _types_pb2.Filters
    group_by: CountGroupBy
    def __init__(self, filters: _Optional[_Union[_types_pb2.Filters, _Mapping]] = ..., group_by: _Optional[_Union[CountGroupBy, str]] = ...) -> None: ...

class CountGroup(_message.Message):
    __slots__ = ("value", "count")
    VALUE_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    value: str
    count: int
    def __init__(self, value: _Optional[str] = ..., count: _Optional[int] = ...) -> None: ...

class CountDocumentsResponse(_message.Message):
    __slots__ = ("total_count", "total_is_exact", "index_version", "policy_version", "groups", "metadata_gaps")
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    TOTAL_IS_EXACT_FIELD_NUMBER: _ClassVar[int]
    INDEX_VERSION_FIELD_NUMBER: _ClassVar[int]
    POLICY_VERSION_FIELD_NUMBER: _ClassVar[int]
    GROUPS_FIELD_NUMBER: _ClassVar[int]
    METADATA_GAPS_FIELD_NUMBER: _ClassVar[int]
    total_count: int
    total_is_exact: bool
    index_version: str
    policy_version: str
    groups: _containers.RepeatedCompositeFieldContainer[CountGroup]
    metadata_gaps: _containers.RepeatedCompositeFieldContainer[MetadataGap]
    def __init__(self, total_count: _Optional[int] = ..., total_is_exact: _Optional[bool] = ..., index_version: _Optional[str] = ..., policy_version: _Optional[str] = ..., groups: _Optional[_Iterable[_Union[CountGroup, _Mapping]]] = ..., metadata_gaps: _Optional[_Iterable[_Union[MetadataGap, _Mapping]]] = ...) -> None: ...

class GetExcerptRequest(_message.Message):
    __slots__ = ("doc_id", "chunk_id")
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    doc_id: str
    chunk_id: str
    def __init__(self, doc_id: _Optional[str] = ..., chunk_id: _Optional[str] = ...) -> None: ...

class GetExcerptResponse(_message.Message):
    __slots__ = ("chunk_id", "doc_id", "doc_version", "text", "span", "title", "heading_path", "source_uri", "source_type", "owner", "classification", "index_version", "policy_version", "injection_flag")
    CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    DOC_VERSION_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    SPAN_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    HEADING_PATH_FIELD_NUMBER: _ClassVar[int]
    SOURCE_URI_FIELD_NUMBER: _ClassVar[int]
    SOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
    INDEX_VERSION_FIELD_NUMBER: _ClassVar[int]
    POLICY_VERSION_FIELD_NUMBER: _ClassVar[int]
    INJECTION_FLAG_FIELD_NUMBER: _ClassVar[int]
    chunk_id: str
    doc_id: str
    doc_version: int
    text: str
    span: _types_pb2.Span
    title: str
    heading_path: _containers.RepeatedScalarFieldContainer[str]
    source_uri: str
    source_type: _types_pb2.SourceType
    owner: str
    classification: _types_pb2.Classification
    index_version: str
    policy_version: str
    injection_flag: bool
    def __init__(self, chunk_id: _Optional[str] = ..., doc_id: _Optional[str] = ..., doc_version: _Optional[int] = ..., text: _Optional[str] = ..., span: _Optional[_Union[_types_pb2.Span, _Mapping]] = ..., title: _Optional[str] = ..., heading_path: _Optional[_Iterable[str]] = ..., source_uri: _Optional[str] = ..., source_type: _Optional[_Union[_types_pb2.SourceType, str]] = ..., owner: _Optional[str] = ..., classification: _Optional[_Union[_types_pb2.Classification, str]] = ..., index_version: _Optional[str] = ..., policy_version: _Optional[str] = ..., injection_flag: _Optional[bool] = ...) -> None: ...

class CheckAccessRequest(_message.Message):
    __slots__ = ("chunk_ids",)
    CHUNK_IDS_FIELD_NUMBER: _ClassVar[int]
    chunk_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, chunk_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class CheckAccessResponse(_message.Message):
    __slots__ = ("results", "checked_at", "policy_version")
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    CHECKED_AT_FIELD_NUMBER: _ClassVar[int]
    POLICY_VERSION_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[AccessResult]
    checked_at: _timestamp_pb2.Timestamp
    policy_version: str
    def __init__(self, results: _Optional[_Iterable[_Union[AccessResult, _Mapping]]] = ..., checked_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., policy_version: _Optional[str] = ...) -> None: ...

class AccessResult(_message.Message):
    __slots__ = ("chunk_id", "allowed", "reason")
    CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    chunk_id: str
    allowed: bool
    reason: AccessReason
    def __init__(self, chunk_id: _Optional[str] = ..., allowed: _Optional[bool] = ..., reason: _Optional[_Union[AccessReason, str]] = ...) -> None: ...

class CheckDocumentAccessRequest(_message.Message):
    __slots__ = ("doc_ids",)
    DOC_IDS_FIELD_NUMBER: _ClassVar[int]
    doc_ids: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, doc_ids: _Optional[_Iterable[str]] = ...) -> None: ...

class CheckDocumentAccessResponse(_message.Message):
    __slots__ = ("results", "checked_at", "policy_version")
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    CHECKED_AT_FIELD_NUMBER: _ClassVar[int]
    POLICY_VERSION_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[DocumentAccessResult]
    checked_at: _timestamp_pb2.Timestamp
    policy_version: str
    def __init__(self, results: _Optional[_Iterable[_Union[DocumentAccessResult, _Mapping]]] = ..., checked_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., policy_version: _Optional[str] = ...) -> None: ...

class DocumentAccessResult(_message.Message):
    __slots__ = ("doc_id", "allowed", "reason")
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    ALLOWED_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    doc_id: str
    allowed: bool
    reason: AccessReason
    def __init__(self, doc_id: _Optional[str] = ..., allowed: _Optional[bool] = ..., reason: _Optional[_Union[AccessReason, str]] = ...) -> None: ...
