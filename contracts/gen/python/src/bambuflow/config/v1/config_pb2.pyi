from bambuflow.common.v1 import types_pb2 as _types_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TermStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    TERM_STATUS_UNSPECIFIED: _ClassVar[TermStatus]
    TERM_STATUS_ACTIVE: _ClassVar[TermStatus]
    TERM_STATUS_DEPRECATED: _ClassVar[TermStatus]
TERM_STATUS_UNSPECIFIED: TermStatus
TERM_STATUS_ACTIVE: TermStatus
TERM_STATUS_DEPRECATED: TermStatus

class GetCandidateRequest(_message.Message):
    __slots__ = ("service", "version_id")
    SERVICE_FIELD_NUMBER: _ClassVar[int]
    VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    service: str
    version_id: str
    def __init__(self, service: _Optional[str] = ..., version_id: _Optional[str] = ...) -> None: ...

class GetCandidateResponse(_message.Message):
    __slots__ = ("snapshot",)
    SNAPSHOT_FIELD_NUMBER: _ClassVar[int]
    snapshot: GetSnapshotResponse
    def __init__(self, snapshot: _Optional[_Union[GetSnapshotResponse, _Mapping]] = ...) -> None: ...

class WatchRequest(_message.Message):
    __slots__ = ("service", "instance_id", "loaded")
    SERVICE_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    LOADED_FIELD_NUMBER: _ClassVar[int]
    service: str
    instance_id: str
    loaded: str
    def __init__(self, service: _Optional[str] = ..., instance_id: _Optional[str] = ..., loaded: _Optional[str] = ...) -> None: ...

class WatchResponse(_message.Message):
    __slots__ = ("service", "snapshot_version")
    SERVICE_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_VERSION_FIELD_NUMBER: _ClassVar[int]
    service: str
    snapshot_version: str
    def __init__(self, service: _Optional[str] = ..., snapshot_version: _Optional[str] = ...) -> None: ...

class AckRequest(_message.Message):
    __slots__ = ("service", "instance_id", "snapshot_version", "loaded", "reason")
    SERVICE_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    SNAPSHOT_VERSION_FIELD_NUMBER: _ClassVar[int]
    LOADED_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    service: str
    instance_id: str
    snapshot_version: str
    loaded: bool
    reason: str
    def __init__(self, service: _Optional[str] = ..., instance_id: _Optional[str] = ..., snapshot_version: _Optional[str] = ..., loaded: _Optional[bool] = ..., reason: _Optional[str] = ...) -> None: ...

class AckResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetSigningKeysRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetSigningKeysResponse(_message.Message):
    __slots__ = ("keys",)
    KEYS_FIELD_NUMBER: _ClassVar[int]
    keys: _containers.RepeatedCompositeFieldContainer[SigningKey]
    def __init__(self, keys: _Optional[_Iterable[_Union[SigningKey, _Mapping]]] = ...) -> None: ...

class SigningKey(_message.Message):
    __slots__ = ("key_id", "public_key")
    KEY_ID_FIELD_NUMBER: _ClassVar[int]
    PUBLIC_KEY_FIELD_NUMBER: _ClassVar[int]
    key_id: str
    public_key: bytes
    def __init__(self, key_id: _Optional[str] = ..., public_key: _Optional[bytes] = ...) -> None: ...

class GetSnapshotRequest(_message.Message):
    __slots__ = ("service",)
    SERVICE_FIELD_NUMBER: _ClassVar[int]
    service: str
    def __init__(self, service: _Optional[str] = ...) -> None: ...

class GetSnapshotResponse(_message.Message):
    __slots__ = ("snapshot_version", "vocabularies", "source_grants", "experiments", "signed_payload", "signature", "signing_key_id", "tenants", "pipelines", "routing", "policy", "prompts", "canaries", "push_sources")
    SNAPSHOT_VERSION_FIELD_NUMBER: _ClassVar[int]
    VOCABULARIES_FIELD_NUMBER: _ClassVar[int]
    SOURCE_GRANTS_FIELD_NUMBER: _ClassVar[int]
    EXPERIMENTS_FIELD_NUMBER: _ClassVar[int]
    SIGNED_PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    SIGNATURE_FIELD_NUMBER: _ClassVar[int]
    SIGNING_KEY_ID_FIELD_NUMBER: _ClassVar[int]
    TENANTS_FIELD_NUMBER: _ClassVar[int]
    PIPELINES_FIELD_NUMBER: _ClassVar[int]
    ROUTING_FIELD_NUMBER: _ClassVar[int]
    POLICY_FIELD_NUMBER: _ClassVar[int]
    PROMPTS_FIELD_NUMBER: _ClassVar[int]
    CANARIES_FIELD_NUMBER: _ClassVar[int]
    PUSH_SOURCES_FIELD_NUMBER: _ClassVar[int]
    snapshot_version: str
    vocabularies: _containers.RepeatedCompositeFieldContainer[Vocabulary]
    source_grants: _containers.RepeatedCompositeFieldContainer[SourceGrant]
    experiments: _containers.RepeatedCompositeFieldContainer[RunningExperiment]
    signed_payload: bytes
    signature: bytes
    signing_key_id: str
    tenants: _containers.RepeatedCompositeFieldContainer[TenantConfig]
    pipelines: _containers.RepeatedCompositeFieldContainer[PipelineConfig]
    routing: RoutingConfig
    policy: PolicyBundle
    prompts: _containers.RepeatedCompositeFieldContainer[PromptTemplate]
    canaries: _containers.RepeatedCompositeFieldContainer[Canary]
    push_sources: _containers.RepeatedCompositeFieldContainer[PushSource]
    def __init__(self, snapshot_version: _Optional[str] = ..., vocabularies: _Optional[_Iterable[_Union[Vocabulary, _Mapping]]] = ..., source_grants: _Optional[_Iterable[_Union[SourceGrant, _Mapping]]] = ..., experiments: _Optional[_Iterable[_Union[RunningExperiment, _Mapping]]] = ..., signed_payload: _Optional[bytes] = ..., signature: _Optional[bytes] = ..., signing_key_id: _Optional[str] = ..., tenants: _Optional[_Iterable[_Union[TenantConfig, _Mapping]]] = ..., pipelines: _Optional[_Iterable[_Union[PipelineConfig, _Mapping]]] = ..., routing: _Optional[_Union[RoutingConfig, _Mapping]] = ..., policy: _Optional[_Union[PolicyBundle, _Mapping]] = ..., prompts: _Optional[_Iterable[_Union[PromptTemplate, _Mapping]]] = ..., canaries: _Optional[_Iterable[_Union[Canary, _Mapping]]] = ..., push_sources: _Optional[_Iterable[_Union[PushSource, _Mapping]]] = ...) -> None: ...

class PushSource(_message.Message):
    __slots__ = ("source_id", "push_client", "default_classification")
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    PUSH_CLIENT_FIELD_NUMBER: _ClassVar[int]
    DEFAULT_CLASSIFICATION_FIELD_NUMBER: _ClassVar[int]
    source_id: str
    push_client: str
    default_classification: _types_pb2.Classification
    def __init__(self, source_id: _Optional[str] = ..., push_client: _Optional[str] = ..., default_classification: _Optional[_Union[_types_pb2.Classification, str]] = ...) -> None: ...

class Canary(_message.Message):
    __slots__ = ("object", "scope", "release_id", "version_id", "tenant", "percent", "pipeline", "routing", "prompt", "policy")
    OBJECT_FIELD_NUMBER: _ClassVar[int]
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    RELEASE_ID_FIELD_NUMBER: _ClassVar[int]
    VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    PERCENT_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_FIELD_NUMBER: _ClassVar[int]
    ROUTING_FIELD_NUMBER: _ClassVar[int]
    PROMPT_FIELD_NUMBER: _ClassVar[int]
    POLICY_FIELD_NUMBER: _ClassVar[int]
    object: str
    scope: str
    release_id: str
    version_id: str
    tenant: str
    percent: int
    pipeline: PipelineConfig
    routing: RoutingConfig
    prompt: PromptTemplate
    policy: PolicyBundle
    def __init__(self, object: _Optional[str] = ..., scope: _Optional[str] = ..., release_id: _Optional[str] = ..., version_id: _Optional[str] = ..., tenant: _Optional[str] = ..., percent: _Optional[int] = ..., pipeline: _Optional[_Union[PipelineConfig, _Mapping]] = ..., routing: _Optional[_Union[RoutingConfig, _Mapping]] = ..., prompt: _Optional[_Union[PromptTemplate, _Mapping]] = ..., policy: _Optional[_Union[PolicyBundle, _Mapping]] = ...) -> None: ...

class PromptTemplate(_message.Message):
    __slots__ = ("scope", "version_id", "system", "user")
    SCOPE_FIELD_NUMBER: _ClassVar[int]
    VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    SYSTEM_FIELD_NUMBER: _ClassVar[int]
    USER_FIELD_NUMBER: _ClassVar[int]
    scope: str
    version_id: str
    system: str
    user: str
    def __init__(self, scope: _Optional[str] = ..., version_id: _Optional[str] = ..., system: _Optional[str] = ..., user: _Optional[str] = ...) -> None: ...

class PolicyBundle(_message.Message):
    __slots__ = ("version", "modules")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    MODULES_FIELD_NUMBER: _ClassVar[int]
    version: str
    modules: _containers.RepeatedCompositeFieldContainer[PolicyModule]
    def __init__(self, version: _Optional[str] = ..., modules: _Optional[_Iterable[_Union[PolicyModule, _Mapping]]] = ...) -> None: ...

class PolicyModule(_message.Message):
    __slots__ = ("path", "source")
    PATH_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    path: str
    source: str
    def __init__(self, path: _Optional[str] = ..., source: _Optional[str] = ...) -> None: ...

class PipelineConfig(_message.Message):
    __slots__ = ("tenant", "version_id", "retrieval", "answer")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    RETRIEVAL_FIELD_NUMBER: _ClassVar[int]
    ANSWER_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    version_id: str
    retrieval: RetrievalPlan
    answer: AnswerPlan
    def __init__(self, tenant: _Optional[str] = ..., version_id: _Optional[str] = ..., retrieval: _Optional[_Union[RetrievalPlan, _Mapping]] = ..., answer: _Optional[_Union[AnswerPlan, _Mapping]] = ...) -> None: ...

class RetrievalPlan(_message.Message):
    __slots__ = ("disabled_routes", "route_weights", "rerank")
    class RouteWeightsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: float
        def __init__(self, key: _Optional[str] = ..., value: _Optional[float] = ...) -> None: ...
    DISABLED_ROUTES_FIELD_NUMBER: _ClassVar[int]
    ROUTE_WEIGHTS_FIELD_NUMBER: _ClassVar[int]
    RERANK_FIELD_NUMBER: _ClassVar[int]
    disabled_routes: _containers.RepeatedScalarFieldContainer[str]
    route_weights: _containers.ScalarMap[str, float]
    rerank: bool
    def __init__(self, disabled_routes: _Optional[_Iterable[str]] = ..., route_weights: _Optional[_Mapping[str, float]] = ..., rerank: _Optional[bool] = ...) -> None: ...

class AnswerPlan(_message.Message):
    __slots__ = ("support_threshold", "min_coverage", "single_hop_llm_rerank", "tenant_rules", "agent_max_rounds", "agent_max_sub_questions")
    SUPPORT_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    MIN_COVERAGE_FIELD_NUMBER: _ClassVar[int]
    SINGLE_HOP_LLM_RERANK_FIELD_NUMBER: _ClassVar[int]
    TENANT_RULES_FIELD_NUMBER: _ClassVar[int]
    AGENT_MAX_ROUNDS_FIELD_NUMBER: _ClassVar[int]
    AGENT_MAX_SUB_QUESTIONS_FIELD_NUMBER: _ClassVar[int]
    support_threshold: float
    min_coverage: float
    single_hop_llm_rerank: bool
    tenant_rules: str
    agent_max_rounds: int
    agent_max_sub_questions: int
    def __init__(self, support_threshold: _Optional[float] = ..., min_coverage: _Optional[float] = ..., single_hop_llm_rerank: _Optional[bool] = ..., tenant_rules: _Optional[str] = ..., agent_max_rounds: _Optional[int] = ..., agent_max_sub_questions: _Optional[int] = ...) -> None: ...

class RoutingConfig(_message.Message):
    __slots__ = ("version_id", "confidence", "generation_model")
    VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    CONFIDENCE_FIELD_NUMBER: _ClassVar[int]
    GENERATION_MODEL_FIELD_NUMBER: _ClassVar[int]
    version_id: str
    confidence: ConfidenceWeights
    generation_model: str
    def __init__(self, version_id: _Optional[str] = ..., confidence: _Optional[_Union[ConfidenceWeights, _Mapping]] = ..., generation_model: _Optional[str] = ...) -> None: ...

class ConfidenceWeights(_message.Message):
    __slots__ = ("version", "coverage", "support", "rerank", "diversity", "conflict", "bias")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    COVERAGE_FIELD_NUMBER: _ClassVar[int]
    SUPPORT_FIELD_NUMBER: _ClassVar[int]
    RERANK_FIELD_NUMBER: _ClassVar[int]
    DIVERSITY_FIELD_NUMBER: _ClassVar[int]
    CONFLICT_FIELD_NUMBER: _ClassVar[int]
    BIAS_FIELD_NUMBER: _ClassVar[int]
    version: str
    coverage: float
    support: float
    rerank: float
    diversity: float
    conflict: float
    bias: float
    def __init__(self, version: _Optional[str] = ..., coverage: _Optional[float] = ..., support: _Optional[float] = ..., rerank: _Optional[float] = ..., diversity: _Optional[float] = ..., conflict: _Optional[float] = ..., bias: _Optional[float] = ...) -> None: ...

class TenantConfig(_message.Message):
    __slots__ = ("tenant", "version_id", "content")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    VERSION_ID_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    version_id: str
    content: str
    def __init__(self, tenant: _Optional[str] = ..., version_id: _Optional[str] = ..., content: _Optional[str] = ...) -> None: ...

class RunningExperiment(_message.Message):
    __slots__ = ("exp_id", "tenant", "traffic_percent", "support_threshold", "min_coverage", "pipeline", "routing", "prompts")
    EXP_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    TRAFFIC_PERCENT_FIELD_NUMBER: _ClassVar[int]
    SUPPORT_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    MIN_COVERAGE_FIELD_NUMBER: _ClassVar[int]
    PIPELINE_FIELD_NUMBER: _ClassVar[int]
    ROUTING_FIELD_NUMBER: _ClassVar[int]
    PROMPTS_FIELD_NUMBER: _ClassVar[int]
    exp_id: str
    tenant: str
    traffic_percent: int
    support_threshold: float
    min_coverage: float
    pipeline: PipelineConfig
    routing: RoutingConfig
    prompts: _containers.RepeatedCompositeFieldContainer[PromptTemplate]
    def __init__(self, exp_id: _Optional[str] = ..., tenant: _Optional[str] = ..., traffic_percent: _Optional[int] = ..., support_threshold: _Optional[float] = ..., min_coverage: _Optional[float] = ..., pipeline: _Optional[_Union[PipelineConfig, _Mapping]] = ..., routing: _Optional[_Union[RoutingConfig, _Mapping]] = ..., prompts: _Optional[_Iterable[_Union[PromptTemplate, _Mapping]]] = ...) -> None: ...

class SourceGrant(_message.Message):
    __slots__ = ("tenant", "source_id")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    SOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    source_id: str
    def __init__(self, tenant: _Optional[str] = ..., source_id: _Optional[str] = ...) -> None: ...

class Vocabulary(_message.Message):
    __slots__ = ("name", "terms")
    NAME_FIELD_NUMBER: _ClassVar[int]
    TERMS_FIELD_NUMBER: _ClassVar[int]
    name: str
    terms: _containers.RepeatedCompositeFieldContainer[VocabularyTerm]
    def __init__(self, name: _Optional[str] = ..., terms: _Optional[_Iterable[_Union[VocabularyTerm, _Mapping]]] = ...) -> None: ...

class VocabularyTerm(_message.Message):
    __slots__ = ("code", "name", "synonyms", "parent", "status", "replaced_by")
    CODE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SYNONYMS_FIELD_NUMBER: _ClassVar[int]
    PARENT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    REPLACED_BY_FIELD_NUMBER: _ClassVar[int]
    code: str
    name: str
    synonyms: _containers.RepeatedScalarFieldContainer[str]
    parent: str
    status: TermStatus
    replaced_by: str
    def __init__(self, code: _Optional[str] = ..., name: _Optional[str] = ..., synonyms: _Optional[_Iterable[str]] = ..., parent: _Optional[str] = ..., status: _Optional[_Union[TermStatus, str]] = ..., replaced_by: _Optional[str] = ...) -> None: ...
