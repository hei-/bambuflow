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

class AnswerKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ANSWER_KIND_UNSPECIFIED: _ClassVar[AnswerKind]
    ANSWER_KIND_FACT: _ClassVar[AnswerKind]
    ANSWER_KIND_ENUMERATION: _ClassVar[AnswerKind]
    ANSWER_KIND_COUNT: _ClassVar[AnswerKind]
    ANSWER_KIND_SUMMARY: _ClassVar[AnswerKind]

class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STATUS_UNSPECIFIED: _ClassVar[Status]
    STATUS_ANSWERED: _ClassVar[Status]
    STATUS_EVIDENCE_ONLY: _ClassVar[Status]
    STATUS_INSUFFICIENT_EVIDENCE: _ClassVar[Status]
    STATUS_REFUSED: _ClassVar[Status]

class ClaimType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CLAIM_TYPE_UNSPECIFIED: _ClassVar[ClaimType]
    CLAIM_TYPE_QUOTE: _ClassVar[ClaimType]
    CLAIM_TYPE_SYNTHESIS: _ClassVar[ClaimType]
    CLAIM_TYPE_CALCULATION: _ClassVar[ClaimType]
    CLAIM_TYPE_LIST_ITEM: _ClassVar[ClaimType]
    CLAIM_TYPE_NONE: _ClassVar[ClaimType]

class BlockType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    BLOCK_TYPE_UNSPECIFIED: _ClassVar[BlockType]
    BLOCK_TYPE_HEADING: _ClassVar[BlockType]
    BLOCK_TYPE_PARAGRAPH: _ClassVar[BlockType]
    BLOCK_TYPE_LIST_ITEM: _ClassVar[BlockType]
    BLOCK_TYPE_STEP: _ClassVar[BlockType]
    BLOCK_TYPE_TABLE: _ClassVar[BlockType]
    BLOCK_TYPE_NOTE: _ClassVar[BlockType]
    BLOCK_TYPE_SCOPE: _ClassVar[BlockType]

class ExperimentArm(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    EXPERIMENT_ARM_UNSPECIFIED: _ClassVar[ExperimentArm]
    EXPERIMENT_ARM_CONTROL: _ClassVar[ExperimentArm]
    EXPERIMENT_ARM_TREATMENT: _ClassVar[ExperimentArm]
    EXPERIMENT_ARM_EXCLUDED: _ClassVar[ExperimentArm]

class Intent(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INTENT_UNSPECIFIED: _ClassVar[Intent]
    INTENT_SINGLE_HOP: _ClassVar[Intent]
    INTENT_MULTI_HOP: _ClassVar[Intent]
    INTENT_RELATION: _ClassVar[Intent]
    INTENT_LISTING: _ClassVar[Intent]
    INTENT_OUT_OF_SCOPE: _ClassVar[Intent]
    INTENT_STRUCTURED: _ClassVar[Intent]

class ReplayAccess(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    REPLAY_ACCESS_UNSPECIFIED: _ClassVar[ReplayAccess]
    REPLAY_ACCESS_ASKER: _ClassVar[ReplayAccess]
    REPLAY_ACCESS_AUDITOR: _ClassVar[ReplayAccess]

class FeedbackRating(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FEEDBACK_RATING_UNSPECIFIED: _ClassVar[FeedbackRating]
    FEEDBACK_RATING_USEFUL: _ClassVar[FeedbackRating]
    FEEDBACK_RATING_WRONG: _ClassVar[FeedbackRating]
    FEEDBACK_RATING_BAD_CITATION: _ClassVar[FeedbackRating]

class ConfidenceLevel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CONFIDENCE_LEVEL_UNSPECIFIED: _ClassVar[ConfidenceLevel]
    CONFIDENCE_LEVEL_LOW: _ClassVar[ConfidenceLevel]
    CONFIDENCE_LEVEL_MEDIUM: _ClassVar[ConfidenceLevel]
    CONFIDENCE_LEVEL_HIGH: _ClassVar[ConfidenceLevel]
ANSWER_KIND_UNSPECIFIED: AnswerKind
ANSWER_KIND_FACT: AnswerKind
ANSWER_KIND_ENUMERATION: AnswerKind
ANSWER_KIND_COUNT: AnswerKind
ANSWER_KIND_SUMMARY: AnswerKind
STATUS_UNSPECIFIED: Status
STATUS_ANSWERED: Status
STATUS_EVIDENCE_ONLY: Status
STATUS_INSUFFICIENT_EVIDENCE: Status
STATUS_REFUSED: Status
CLAIM_TYPE_UNSPECIFIED: ClaimType
CLAIM_TYPE_QUOTE: ClaimType
CLAIM_TYPE_SYNTHESIS: ClaimType
CLAIM_TYPE_CALCULATION: ClaimType
CLAIM_TYPE_LIST_ITEM: ClaimType
CLAIM_TYPE_NONE: ClaimType
BLOCK_TYPE_UNSPECIFIED: BlockType
BLOCK_TYPE_HEADING: BlockType
BLOCK_TYPE_PARAGRAPH: BlockType
BLOCK_TYPE_LIST_ITEM: BlockType
BLOCK_TYPE_STEP: BlockType
BLOCK_TYPE_TABLE: BlockType
BLOCK_TYPE_NOTE: BlockType
BLOCK_TYPE_SCOPE: BlockType
EXPERIMENT_ARM_UNSPECIFIED: ExperimentArm
EXPERIMENT_ARM_CONTROL: ExperimentArm
EXPERIMENT_ARM_TREATMENT: ExperimentArm
EXPERIMENT_ARM_EXCLUDED: ExperimentArm
INTENT_UNSPECIFIED: Intent
INTENT_SINGLE_HOP: Intent
INTENT_MULTI_HOP: Intent
INTENT_RELATION: Intent
INTENT_LISTING: Intent
INTENT_OUT_OF_SCOPE: Intent
INTENT_STRUCTURED: Intent
REPLAY_ACCESS_UNSPECIFIED: ReplayAccess
REPLAY_ACCESS_ASKER: ReplayAccess
REPLAY_ACCESS_AUDITOR: ReplayAccess
FEEDBACK_RATING_UNSPECIFIED: FeedbackRating
FEEDBACK_RATING_USEFUL: FeedbackRating
FEEDBACK_RATING_WRONG: FeedbackRating
FEEDBACK_RATING_BAD_CITATION: FeedbackRating
CONFIDENCE_LEVEL_UNSPECIFIED: ConfidenceLevel
CONFIDENCE_LEVEL_LOW: ConfidenceLevel
CONFIDENCE_LEVEL_MEDIUM: ConfidenceLevel
CONFIDENCE_LEVEL_HIGH: ConfidenceLevel

class DeleteConversationRequest(_message.Message):
    __slots__ = ("conversation_id",)
    CONVERSATION_ID_FIELD_NUMBER: _ClassVar[int]
    conversation_id: str
    def __init__(self, conversation_id: _Optional[str] = ...) -> None: ...

class DeleteConversationResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class AnswerRequest(_message.Message):
    __slots__ = ("question", "filters", "max_citations", "conversation_id")
    QUESTION_FIELD_NUMBER: _ClassVar[int]
    FILTERS_FIELD_NUMBER: _ClassVar[int]
    MAX_CITATIONS_FIELD_NUMBER: _ClassVar[int]
    CONVERSATION_ID_FIELD_NUMBER: _ClassVar[int]
    question: str
    filters: _types_pb2.Filters
    max_citations: int
    conversation_id: str
    def __init__(self, question: _Optional[str] = ..., filters: _Optional[_Union[_types_pb2.Filters, _Mapping]] = ..., max_citations: _Optional[int] = ..., conversation_id: _Optional[str] = ...) -> None: ...

class StreamAnswerRequest(_message.Message):
    __slots__ = ("request",)
    REQUEST_FIELD_NUMBER: _ClassVar[int]
    request: AnswerRequest
    def __init__(self, request: _Optional[_Union[AnswerRequest, _Mapping]] = ...) -> None: ...

class StreamAnswerResponse(_message.Message):
    __slots__ = ("meta", "citation", "sentence", "final")
    META_FIELD_NUMBER: _ClassVar[int]
    CITATION_FIELD_NUMBER: _ClassVar[int]
    SENTENCE_FIELD_NUMBER: _ClassVar[int]
    FINAL_FIELD_NUMBER: _ClassVar[int]
    meta: AnswerMeta
    citation: Citation
    sentence: SentenceEvent
    final: AnswerFinal
    def __init__(self, meta: _Optional[_Union[AnswerMeta, _Mapping]] = ..., citation: _Optional[_Union[Citation, _Mapping]] = ..., sentence: _Optional[_Union[SentenceEvent, _Mapping]] = ..., final: _Optional[_Union[AnswerFinal, _Mapping]] = ...) -> None: ...

class AnswerMeta(_message.Message):
    __slots__ = ("answer_id", "trace_id", "conversation_id")
    ANSWER_ID_FIELD_NUMBER: _ClassVar[int]
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    CONVERSATION_ID_FIELD_NUMBER: _ClassVar[int]
    answer_id: str
    trace_id: str
    conversation_id: str
    def __init__(self, answer_id: _Optional[str] = ..., trace_id: _Optional[str] = ..., conversation_id: _Optional[str] = ...) -> None: ...

class SentenceEvent(_message.Message):
    __slots__ = ("sentence", "block_type", "scope")
    SENTENCE_FIELD_NUMBER: _ClassVar[int]
    BLOCK_TYPE_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    sentence: Sentence
    block_type: BlockType
    scope: Scope
    def __init__(self, sentence: _Optional[_Union[Sentence, _Mapping]] = ..., block_type: _Optional[_Union[BlockType, str]] = ..., scope: _Optional[_Union[Scope, _Mapping]] = ...) -> None: ...

class AnswerFinal(_message.Message):
    __slots__ = ("status", "why", "audit", "answer_kind", "usage", "confidence")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    WHY_FIELD_NUMBER: _ClassVar[int]
    AUDIT_FIELD_NUMBER: _ClassVar[int]
    ANSWER_KIND_FIELD_NUMBER: _ClassVar[int]
    USAGE_FIELD_NUMBER: _ClassVar[int]
    CONFIDENCE_FIELD_NUMBER: _ClassVar[int]
    status: Status
    why: Why
    audit: AuditBlocks
    answer_kind: AnswerKind
    usage: TokenUsage
    confidence: Confidence
    def __init__(self, status: _Optional[_Union[Status, str]] = ..., why: _Optional[_Union[Why, _Mapping]] = ..., audit: _Optional[_Union[AuditBlocks, _Mapping]] = ..., answer_kind: _Optional[_Union[AnswerKind, str]] = ..., usage: _Optional[_Union[TokenUsage, _Mapping]] = ..., confidence: _Optional[_Union[Confidence, _Mapping]] = ...) -> None: ...

class AuditBlocks(_message.Message):
    __slots__ = ("candidate_chunk_ids", "context_chunk_ids", "shown_chunk_ids", "listed_doc_ids", "total_count")
    CANDIDATE_CHUNK_IDS_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_CHUNK_IDS_FIELD_NUMBER: _ClassVar[int]
    SHOWN_CHUNK_IDS_FIELD_NUMBER: _ClassVar[int]
    LISTED_DOC_IDS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    candidate_chunk_ids: _containers.RepeatedScalarFieldContainer[str]
    context_chunk_ids: _containers.RepeatedScalarFieldContainer[str]
    shown_chunk_ids: _containers.RepeatedScalarFieldContainer[str]
    listed_doc_ids: _containers.RepeatedScalarFieldContainer[str]
    total_count: int
    def __init__(self, candidate_chunk_ids: _Optional[_Iterable[str]] = ..., context_chunk_ids: _Optional[_Iterable[str]] = ..., shown_chunk_ids: _Optional[_Iterable[str]] = ..., listed_doc_ids: _Optional[_Iterable[str]] = ..., total_count: _Optional[int] = ...) -> None: ...

class TokenUsage(_message.Message):
    __slots__ = ("prompt_tokens", "completion_tokens")
    PROMPT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    COMPLETION_TOKENS_FIELD_NUMBER: _ClassVar[int]
    prompt_tokens: int
    completion_tokens: int
    def __init__(self, prompt_tokens: _Optional[int] = ..., completion_tokens: _Optional[int] = ...) -> None: ...

class AnswerResponse(_message.Message):
    __slots__ = ("answer_id", "trace_id", "status", "answer", "sentences", "blocks", "citations", "why", "audit", "answer_kind", "conversation_id", "usage", "confidence")
    ANSWER_ID_FIELD_NUMBER: _ClassVar[int]
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ANSWER_FIELD_NUMBER: _ClassVar[int]
    SENTENCES_FIELD_NUMBER: _ClassVar[int]
    BLOCKS_FIELD_NUMBER: _ClassVar[int]
    CITATIONS_FIELD_NUMBER: _ClassVar[int]
    WHY_FIELD_NUMBER: _ClassVar[int]
    AUDIT_FIELD_NUMBER: _ClassVar[int]
    ANSWER_KIND_FIELD_NUMBER: _ClassVar[int]
    CONVERSATION_ID_FIELD_NUMBER: _ClassVar[int]
    USAGE_FIELD_NUMBER: _ClassVar[int]
    CONFIDENCE_FIELD_NUMBER: _ClassVar[int]
    answer_id: str
    trace_id: str
    status: Status
    answer: str
    sentences: _containers.RepeatedCompositeFieldContainer[Sentence]
    blocks: _containers.RepeatedCompositeFieldContainer[Block]
    citations: _containers.RepeatedCompositeFieldContainer[Citation]
    why: Why
    audit: AuditBlocks
    answer_kind: AnswerKind
    conversation_id: str
    usage: TokenUsage
    confidence: Confidence
    def __init__(self, answer_id: _Optional[str] = ..., trace_id: _Optional[str] = ..., status: _Optional[_Union[Status, str]] = ..., answer: _Optional[str] = ..., sentences: _Optional[_Iterable[_Union[Sentence, _Mapping]]] = ..., blocks: _Optional[_Iterable[_Union[Block, _Mapping]]] = ..., citations: _Optional[_Iterable[_Union[Citation, _Mapping]]] = ..., why: _Optional[_Union[Why, _Mapping]] = ..., audit: _Optional[_Union[AuditBlocks, _Mapping]] = ..., answer_kind: _Optional[_Union[AnswerKind, str]] = ..., conversation_id: _Optional[str] = ..., usage: _Optional[_Union[TokenUsage, _Mapping]] = ..., confidence: _Optional[_Union[Confidence, _Mapping]] = ...) -> None: ...

class Sentence(_message.Message):
    __slots__ = ("text", "start", "end", "claim_type", "ref_ids", "support", "block_id", "quotes", "calc", "document")
    TEXT_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    CLAIM_TYPE_FIELD_NUMBER: _ClassVar[int]
    REF_IDS_FIELD_NUMBER: _ClassVar[int]
    SUPPORT_FIELD_NUMBER: _ClassVar[int]
    BLOCK_ID_FIELD_NUMBER: _ClassVar[int]
    QUOTES_FIELD_NUMBER: _ClassVar[int]
    CALC_FIELD_NUMBER: _ClassVar[int]
    DOCUMENT_FIELD_NUMBER: _ClassVar[int]
    text: str
    start: int
    end: int
    claim_type: ClaimType
    ref_ids: _containers.RepeatedScalarFieldContainer[int]
    support: float
    block_id: str
    quotes: _containers.RepeatedCompositeFieldContainer[RefQuote]
    calc: Calculation
    document: ListedDocument
    def __init__(self, text: _Optional[str] = ..., start: _Optional[int] = ..., end: _Optional[int] = ..., claim_type: _Optional[_Union[ClaimType, str]] = ..., ref_ids: _Optional[_Iterable[int]] = ..., support: _Optional[float] = ..., block_id: _Optional[str] = ..., quotes: _Optional[_Iterable[_Union[RefQuote, _Mapping]]] = ..., calc: _Optional[_Union[Calculation, _Mapping]] = ..., document: _Optional[_Union[ListedDocument, _Mapping]] = ...) -> None: ...

class ListedDocument(_message.Message):
    __slots__ = ("doc_id", "doc_version", "title", "source_uri", "metadata", "classification")
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    DOC_VERSION_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_URI_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
    doc_id: str
    doc_version: int
    title: str
    source_uri: str
    metadata: _types_pb2.DocMetadata
    classification: _types_pb2.Classification
    def __init__(self, doc_id: _Optional[str] = ..., doc_version: _Optional[int] = ..., title: _Optional[str] = ..., source_uri: _Optional[str] = ..., metadata: _Optional[_Union[_types_pb2.DocMetadata, _Mapping]] = ..., classification: _Optional[_Union[_types_pb2.Classification, str]] = ...) -> None: ...

class Calculation(_message.Message):
    __slots__ = ("inputs", "results")
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    inputs: _containers.RepeatedCompositeFieldContainer[CalcInput]
    results: _containers.RepeatedCompositeFieldContainer[CalcResult]
    def __init__(self, inputs: _Optional[_Iterable[_Union[CalcInput, _Mapping]]] = ..., results: _Optional[_Iterable[_Union[CalcResult, _Mapping]]] = ...) -> None: ...

class CalcInput(_message.Message):
    __slots__ = ("var", "ref_id", "value", "unit")
    VAR_FIELD_NUMBER: _ClassVar[int]
    REF_ID_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    UNIT_FIELD_NUMBER: _ClassVar[int]
    var: str
    ref_id: int
    value: str
    unit: str
    def __init__(self, var: _Optional[str] = ..., ref_id: _Optional[int] = ..., value: _Optional[str] = ..., unit: _Optional[str] = ...) -> None: ...

class CalcResult(_message.Message):
    __slots__ = ("name", "expr", "value")
    NAME_FIELD_NUMBER: _ClassVar[int]
    EXPR_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    name: str
    expr: str
    value: str
    def __init__(self, name: _Optional[str] = ..., expr: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...

class RefQuote(_message.Message):
    __slots__ = ("ref_id", "quote")
    REF_ID_FIELD_NUMBER: _ClassVar[int]
    QUOTE_FIELD_NUMBER: _ClassVar[int]
    ref_id: int
    quote: str
    def __init__(self, ref_id: _Optional[int] = ..., quote: _Optional[str] = ...) -> None: ...

class Block(_message.Message):
    __slots__ = ("block_id", "block_type", "parent_block_id", "order", "sentence_indexes", "scope")
    BLOCK_ID_FIELD_NUMBER: _ClassVar[int]
    BLOCK_TYPE_FIELD_NUMBER: _ClassVar[int]
    PARENT_BLOCK_ID_FIELD_NUMBER: _ClassVar[int]
    ORDER_FIELD_NUMBER: _ClassVar[int]
    SENTENCE_INDEXES_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    block_id: str
    block_type: BlockType
    parent_block_id: str
    order: int
    sentence_indexes: _containers.RepeatedScalarFieldContainer[int]
    scope: Scope
    def __init__(self, block_id: _Optional[str] = ..., block_type: _Optional[_Union[BlockType, str]] = ..., parent_block_id: _Optional[str] = ..., order: _Optional[int] = ..., sentence_indexes: _Optional[_Iterable[int]] = ..., scope: _Optional[_Union[Scope, _Mapping]] = ...) -> None: ...

class Scope(_message.Message):
    __slots__ = ("filters_text", "total_count", "total_is_exact", "shown", "metadata_gaps", "basis")
    FILTERS_TEXT_FIELD_NUMBER: _ClassVar[int]
    TOTAL_COUNT_FIELD_NUMBER: _ClassVar[int]
    TOTAL_IS_EXACT_FIELD_NUMBER: _ClassVar[int]
    SHOWN_FIELD_NUMBER: _ClassVar[int]
    METADATA_GAPS_FIELD_NUMBER: _ClassVar[int]
    BASIS_FIELD_NUMBER: _ClassVar[int]
    filters_text: str
    total_count: int
    total_is_exact: bool
    shown: int
    metadata_gaps: str
    basis: str
    def __init__(self, filters_text: _Optional[str] = ..., total_count: _Optional[int] = ..., total_is_exact: _Optional[bool] = ..., shown: _Optional[int] = ..., metadata_gaps: _Optional[str] = ..., basis: _Optional[str] = ...) -> None: ...

class Citation(_message.Message):
    __slots__ = ("ref_id", "chunk_id", "doc_id", "doc_version", "title", "quote", "span", "source_uri", "owner", "classification", "source_type", "injection_flag")
    REF_ID_FIELD_NUMBER: _ClassVar[int]
    CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    DOC_ID_FIELD_NUMBER: _ClassVar[int]
    DOC_VERSION_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    QUOTE_FIELD_NUMBER: _ClassVar[int]
    SPAN_FIELD_NUMBER: _ClassVar[int]
    SOURCE_URI_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
    SOURCE_TYPE_FIELD_NUMBER: _ClassVar[int]
    INJECTION_FLAG_FIELD_NUMBER: _ClassVar[int]
    ref_id: int
    chunk_id: str
    doc_id: str
    doc_version: int
    title: str
    quote: str
    span: _types_pb2.Span
    source_uri: str
    owner: str
    classification: _types_pb2.Classification
    source_type: _types_pb2.SourceType
    injection_flag: bool
    def __init__(self, ref_id: _Optional[int] = ..., chunk_id: _Optional[str] = ..., doc_id: _Optional[str] = ..., doc_version: _Optional[int] = ..., title: _Optional[str] = ..., quote: _Optional[str] = ..., span: _Optional[_Union[_types_pb2.Span, _Mapping]] = ..., source_uri: _Optional[str] = ..., owner: _Optional[str] = ..., classification: _Optional[_Union[_types_pb2.Classification, str]] = ..., source_type: _Optional[_Union[_types_pb2.SourceType, str]] = ..., injection_flag: _Optional[bool] = ...) -> None: ...

class Why(_message.Message):
    __slots__ = ("retrievers", "index_version", "policy_version", "embedding_model", "generation_model", "prompt_version", "nli_model", "support_threshold", "degraded", "rerank_model", "calculator_version", "inferred_filters", "intent", "agent_rounds", "graph_path", "based_on_summaries", "experiment_id", "experiment_arm", "evidence_scores", "llm_rerank", "pipeline_version", "routing_version", "canary_releases")
    RETRIEVERS_FIELD_NUMBER: _ClassVar[int]
    INDEX_VERSION_FIELD_NUMBER: _ClassVar[int]
    POLICY_VERSION_FIELD_NUMBER: _ClassVar[int]
    EMBEDDING_MODEL_FIELD_NUMBER: _ClassVar[int]
    GENERATION_MODEL_FIELD_NUMBER: _ClassVar[int]
    PROMPT_VERSION_FIELD_NUMBER: _ClassVar[int]
    NLI_MODEL_FIELD_NUMBER: _ClassVar[int]
    SUPPORT_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    DEGRADED_FIELD_NUMBER: _ClassVar[int]
    RERANK_MODEL_FIELD_NUMBER: _ClassVar[int]
    CALCULATOR_VERSION_FIELD_NUMBER: _ClassVar[int]
    INFERRED_FILTERS_FIELD_NUMBER: _ClassVar[int]
    INTENT_FIELD_NUMBER: _ClassVar[int]
    AGENT_ROUNDS_FIELD_NUMBER: _ClassVar[int]
    GRAPH_PATH_FIELD_NUMBER: _ClassVar[int]
    BASED_ON_SUMMARIES_FIELD_NUMBER: _ClassVar[int]
    EXPERIMENT_ID_FIELD_NUMBER: _ClassVar[int]
    EXPERIMENT_ARM_FIELD_NUMBER: _ClassVar[int]
    EVIDENCE_SCORES_FIELD_NUMBER: _ClassVar[int]
    LLM_RERANK_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_VERSION_FIELD_NUMBER: _ClassVar[int]
    ROUTING_VERSION_FIELD_NUMBER: _ClassVar[int]
    CANARY_RELEASES_FIELD_NUMBER: _ClassVar[int]
    retrievers: _containers.RepeatedScalarFieldContainer[str]
    index_version: str
    policy_version: str
    embedding_model: str
    generation_model: str
    prompt_version: str
    nli_model: str
    support_threshold: float
    degraded: _containers.RepeatedCompositeFieldContainer[_types_pb2.Degradation]
    rerank_model: str
    calculator_version: str
    inferred_filters: _types_pb2.Filters
    intent: Intent
    agent_rounds: _containers.RepeatedCompositeFieldContainer[AgentRound]
    graph_path: _containers.RepeatedCompositeFieldContainer[GraphPath]
    based_on_summaries: bool
    experiment_id: str
    experiment_arm: ExperimentArm
    evidence_scores: _containers.RepeatedCompositeFieldContainer[EvidenceScore]
    llm_rerank: str
    pipeline_version: str
    routing_version: str
    canary_releases: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, retrievers: _Optional[_Iterable[str]] = ..., index_version: _Optional[str] = ..., policy_version: _Optional[str] = ..., embedding_model: _Optional[str] = ..., generation_model: _Optional[str] = ..., prompt_version: _Optional[str] = ..., nli_model: _Optional[str] = ..., support_threshold: _Optional[float] = ..., degraded: _Optional[_Iterable[_Union[_types_pb2.Degradation, _Mapping]]] = ..., rerank_model: _Optional[str] = ..., calculator_version: _Optional[str] = ..., inferred_filters: _Optional[_Union[_types_pb2.Filters, _Mapping]] = ..., intent: _Optional[_Union[Intent, str]] = ..., agent_rounds: _Optional[_Iterable[_Union[AgentRound, _Mapping]]] = ..., graph_path: _Optional[_Iterable[_Union[GraphPath, _Mapping]]] = ..., based_on_summaries: _Optional[bool] = ..., experiment_id: _Optional[str] = ..., experiment_arm: _Optional[_Union[ExperimentArm, str]] = ..., evidence_scores: _Optional[_Iterable[_Union[EvidenceScore, _Mapping]]] = ..., llm_rerank: _Optional[str] = ..., pipeline_version: _Optional[str] = ..., routing_version: _Optional[str] = ..., canary_releases: _Optional[_Iterable[str]] = ...) -> None: ...

class EvidenceScore(_message.Message):
    __slots__ = ("ref_id", "chunk_id", "matched_by", "score_details")
    REF_ID_FIELD_NUMBER: _ClassVar[int]
    CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    MATCHED_BY_FIELD_NUMBER: _ClassVar[int]
    SCORE_DETAILS_FIELD_NUMBER: _ClassVar[int]
    ref_id: int
    chunk_id: str
    matched_by: _containers.RepeatedCompositeFieldContainer[EvidenceMatch]
    score_details: EvidenceScoreDetails
    def __init__(self, ref_id: _Optional[int] = ..., chunk_id: _Optional[str] = ..., matched_by: _Optional[_Iterable[_Union[EvidenceMatch, _Mapping]]] = ..., score_details: _Optional[_Union[EvidenceScoreDetails, _Mapping]] = ...) -> None: ...

class EvidenceMatch(_message.Message):
    __slots__ = ("route", "query_id", "rank")
    ROUTE_FIELD_NUMBER: _ClassVar[int]
    QUERY_ID_FIELD_NUMBER: _ClassVar[int]
    RANK_FIELD_NUMBER: _ClassVar[int]
    route: str
    query_id: str
    rank: int
    def __init__(self, route: _Optional[str] = ..., query_id: _Optional[str] = ..., rank: _Optional[int] = ...) -> None: ...

class EvidenceScoreDetails(_message.Message):
    __slots__ = ("keyword", "dense", "graph", "identifier", "rrf", "rerank", "sparse")
    KEYWORD_FIELD_NUMBER: _ClassVar[int]
    DENSE_FIELD_NUMBER: _ClassVar[int]
    GRAPH_FIELD_NUMBER: _ClassVar[int]
    IDENTIFIER_FIELD_NUMBER: _ClassVar[int]
    RRF_FIELD_NUMBER: _ClassVar[int]
    RERANK_FIELD_NUMBER: _ClassVar[int]
    SPARSE_FIELD_NUMBER: _ClassVar[int]
    keyword: float
    dense: float
    graph: float
    identifier: float
    rrf: float
    rerank: float
    sparse: float
    def __init__(self, keyword: _Optional[float] = ..., dense: _Optional[float] = ..., graph: _Optional[float] = ..., identifier: _Optional[float] = ..., rrf: _Optional[float] = ..., rerank: _Optional[float] = ..., sparse: _Optional[float] = ...) -> None: ...

class GraphPath(_message.Message):
    __slots__ = ("ref_id", "chunk_id", "edges")
    REF_ID_FIELD_NUMBER: _ClassVar[int]
    CHUNK_ID_FIELD_NUMBER: _ClassVar[int]
    EDGES_FIELD_NUMBER: _ClassVar[int]
    ref_id: int
    chunk_id: str
    edges: _containers.RepeatedCompositeFieldContainer[GraphEdge]
    def __init__(self, ref_id: _Optional[int] = ..., chunk_id: _Optional[str] = ..., edges: _Optional[_Iterable[_Union[GraphEdge, _Mapping]]] = ...) -> None: ...

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

class AgentRound(_message.Message):
    __slots__ = ("round", "calls")
    ROUND_FIELD_NUMBER: _ClassVar[int]
    CALLS_FIELD_NUMBER: _ClassVar[int]
    round: int
    calls: _containers.RepeatedCompositeFieldContainer[AgentCall]
    def __init__(self, round: _Optional[int] = ..., calls: _Optional[_Iterable[_Union[AgentCall, _Mapping]]] = ...) -> None: ...

class AgentCall(_message.Message):
    __slots__ = ("sub_question_id", "sub_question", "tool", "chunk_ids", "covered", "elapsed_ms")
    SUB_QUESTION_ID_FIELD_NUMBER: _ClassVar[int]
    SUB_QUESTION_FIELD_NUMBER: _ClassVar[int]
    TOOL_FIELD_NUMBER: _ClassVar[int]
    CHUNK_IDS_FIELD_NUMBER: _ClassVar[int]
    COVERED_FIELD_NUMBER: _ClassVar[int]
    ELAPSED_MS_FIELD_NUMBER: _ClassVar[int]
    sub_question_id: str
    sub_question: str
    tool: str
    chunk_ids: _containers.RepeatedScalarFieldContainer[str]
    covered: bool
    elapsed_ms: int
    def __init__(self, sub_question_id: _Optional[str] = ..., sub_question: _Optional[str] = ..., tool: _Optional[str] = ..., chunk_ids: _Optional[_Iterable[str]] = ..., covered: _Optional[bool] = ..., elapsed_ms: _Optional[int] = ...) -> None: ...

class GetAnswerRequest(_message.Message):
    __slots__ = ("answer_id", "as_auditor")
    ANSWER_ID_FIELD_NUMBER: _ClassVar[int]
    AS_AUDITOR_FIELD_NUMBER: _ClassVar[int]
    answer_id: str
    as_auditor: bool
    def __init__(self, answer_id: _Optional[str] = ..., as_auditor: _Optional[bool] = ...) -> None: ...

class GetAnswerResponse(_message.Message):
    __slots__ = ("answer", "question", "created_at", "body_withheld", "access")
    ANSWER_FIELD_NUMBER: _ClassVar[int]
    QUESTION_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    BODY_WITHHELD_FIELD_NUMBER: _ClassVar[int]
    ACCESS_FIELD_NUMBER: _ClassVar[int]
    answer: AnswerResponse
    question: str
    created_at: _timestamp_pb2.Timestamp
    body_withheld: bool
    access: ReplayAccess
    def __init__(self, answer: _Optional[_Union[AnswerResponse, _Mapping]] = ..., question: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., body_withheld: _Optional[bool] = ..., access: _Optional[_Union[ReplayAccess, str]] = ...) -> None: ...

class CreateFeedbackRequest(_message.Message):
    __slots__ = ("answer_id", "rating", "sentence_index", "comment", "idempotency_key")
    ANSWER_ID_FIELD_NUMBER: _ClassVar[int]
    RATING_FIELD_NUMBER: _ClassVar[int]
    SENTENCE_INDEX_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    answer_id: str
    rating: FeedbackRating
    sentence_index: int
    comment: str
    idempotency_key: str
    def __init__(self, answer_id: _Optional[str] = ..., rating: _Optional[_Union[FeedbackRating, str]] = ..., sentence_index: _Optional[int] = ..., comment: _Optional[str] = ..., idempotency_key: _Optional[str] = ...) -> None: ...

class CreateFeedbackResponse(_message.Message):
    __slots__ = ("feedback",)
    FEEDBACK_FIELD_NUMBER: _ClassVar[int]
    feedback: Feedback
    def __init__(self, feedback: _Optional[_Union[Feedback, _Mapping]] = ...) -> None: ...

class Feedback(_message.Message):
    __slots__ = ("feedback_id", "answer_id", "rating", "sentence_index", "created_at")
    FEEDBACK_ID_FIELD_NUMBER: _ClassVar[int]
    ANSWER_ID_FIELD_NUMBER: _ClassVar[int]
    RATING_FIELD_NUMBER: _ClassVar[int]
    SENTENCE_INDEX_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    feedback_id: str
    answer_id: str
    rating: FeedbackRating
    sentence_index: int
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, feedback_id: _Optional[str] = ..., answer_id: _Optional[str] = ..., rating: _Optional[_Union[FeedbackRating, str]] = ..., sentence_index: _Optional[int] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class Confidence(_message.Message):
    __slots__ = ("score", "level", "weights_version", "coverage", "mean_support", "top_rerank", "source_diversity", "conflict")
    SCORE_FIELD_NUMBER: _ClassVar[int]
    LEVEL_FIELD_NUMBER: _ClassVar[int]
    WEIGHTS_VERSION_FIELD_NUMBER: _ClassVar[int]
    COVERAGE_FIELD_NUMBER: _ClassVar[int]
    MEAN_SUPPORT_FIELD_NUMBER: _ClassVar[int]
    TOP_RERANK_FIELD_NUMBER: _ClassVar[int]
    SOURCE_DIVERSITY_FIELD_NUMBER: _ClassVar[int]
    CONFLICT_FIELD_NUMBER: _ClassVar[int]
    score: float
    level: ConfidenceLevel
    weights_version: str
    coverage: float
    mean_support: float
    top_rerank: float
    source_diversity: float
    conflict: bool
    def __init__(self, score: _Optional[float] = ..., level: _Optional[_Union[ConfidenceLevel, str]] = ..., weights_version: _Optional[str] = ..., coverage: _Optional[float] = ..., mean_support: _Optional[float] = ..., top_rerank: _Optional[float] = ..., source_diversity: _Optional[float] = ..., conflict: _Optional[bool] = ...) -> None: ...
