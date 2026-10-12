import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class FeedbackCase(_message.Message):
    __slots__ = ("answer_id", "tenant", "answered_at", "masked_question", "answer_status", "ratings", "comments", "cited_doc_ids", "cited_titles", "status", "resolved_by", "resolution_note", "resolved_at")
    ANSWER_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    ANSWERED_AT_FIELD_NUMBER: _ClassVar[int]
    MASKED_QUESTION_FIELD_NUMBER: _ClassVar[int]
    ANSWER_STATUS_FIELD_NUMBER: _ClassVar[int]
    RATINGS_FIELD_NUMBER: _ClassVar[int]
    COMMENTS_FIELD_NUMBER: _ClassVar[int]
    CITED_DOC_IDS_FIELD_NUMBER: _ClassVar[int]
    CITED_TITLES_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    RESOLVED_BY_FIELD_NUMBER: _ClassVar[int]
    RESOLUTION_NOTE_FIELD_NUMBER: _ClassVar[int]
    RESOLVED_AT_FIELD_NUMBER: _ClassVar[int]
    answer_id: str
    tenant: str
    answered_at: _timestamp_pb2.Timestamp
    masked_question: str
    answer_status: str
    ratings: _containers.RepeatedScalarFieldContainer[str]
    comments: _containers.RepeatedScalarFieldContainer[str]
    cited_doc_ids: _containers.RepeatedScalarFieldContainer[str]
    cited_titles: _containers.RepeatedScalarFieldContainer[str]
    status: str
    resolved_by: str
    resolution_note: str
    resolved_at: _timestamp_pb2.Timestamp
    def __init__(self, answer_id: _Optional[str] = ..., tenant: _Optional[str] = ..., answered_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., masked_question: _Optional[str] = ..., answer_status: _Optional[str] = ..., ratings: _Optional[_Iterable[str]] = ..., comments: _Optional[_Iterable[str]] = ..., cited_doc_ids: _Optional[_Iterable[str]] = ..., cited_titles: _Optional[_Iterable[str]] = ..., status: _Optional[str] = ..., resolved_by: _Optional[str] = ..., resolution_note: _Optional[str] = ..., resolved_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ListFeedbackCasesRequest(_message.Message):
    __slots__ = ("tenant", "status")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    status: str
    def __init__(self, tenant: _Optional[str] = ..., status: _Optional[str] = ...) -> None: ...

class ListFeedbackCasesResponse(_message.Message):
    __slots__ = ("cases",)
    CASES_FIELD_NUMBER: _ClassVar[int]
    cases: _containers.RepeatedCompositeFieldContainer[FeedbackCase]
    def __init__(self, cases: _Optional[_Iterable[_Union[FeedbackCase, _Mapping]]] = ...) -> None: ...

class ResolveFeedbackCaseRequest(_message.Message):
    __slots__ = ("tenant", "answer_id", "resolution", "note")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    ANSWER_ID_FIELD_NUMBER: _ClassVar[int]
    RESOLUTION_FIELD_NUMBER: _ClassVar[int]
    NOTE_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    answer_id: str
    resolution: str
    note: str
    def __init__(self, tenant: _Optional[str] = ..., answer_id: _Optional[str] = ..., resolution: _Optional[str] = ..., note: _Optional[str] = ...) -> None: ...

class ResolveFeedbackCaseResponse(_message.Message):
    __slots__ = ("case",)
    CASE_FIELD_NUMBER: _ClassVar[int]
    case: FeedbackCase
    def __init__(self, case: _Optional[_Union[FeedbackCase, _Mapping]] = ...) -> None: ...

class GoldenExpected(_message.Message):
    __slots__ = ("status", "key_points", "must_cite", "must_not_cite")
    STATUS_FIELD_NUMBER: _ClassVar[int]
    KEY_POINTS_FIELD_NUMBER: _ClassVar[int]
    MUST_CITE_FIELD_NUMBER: _ClassVar[int]
    MUST_NOT_CITE_FIELD_NUMBER: _ClassVar[int]
    status: str
    key_points: _containers.RepeatedScalarFieldContainer[str]
    must_cite: _containers.RepeatedScalarFieldContainer[str]
    must_not_cite: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, status: _Optional[str] = ..., key_points: _Optional[_Iterable[str]] = ..., must_cite: _Optional[_Iterable[str]] = ..., must_not_cite: _Optional[_Iterable[str]] = ...) -> None: ...

class GoldenQuestion(_message.Message):
    __slots__ = ("question_id", "golden_set", "bucket", "asker", "question", "expected", "status", "origin", "submitted_by", "submitted_at", "expert_reviewer", "platform_reviewer", "reject_reason")
    QUESTION_ID_FIELD_NUMBER: _ClassVar[int]
    GOLDEN_SET_FIELD_NUMBER: _ClassVar[int]
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    ASKER_FIELD_NUMBER: _ClassVar[int]
    QUESTION_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ORIGIN_FIELD_NUMBER: _ClassVar[int]
    SUBMITTED_BY_FIELD_NUMBER: _ClassVar[int]
    SUBMITTED_AT_FIELD_NUMBER: _ClassVar[int]
    EXPERT_REVIEWER_FIELD_NUMBER: _ClassVar[int]
    PLATFORM_REVIEWER_FIELD_NUMBER: _ClassVar[int]
    REJECT_REASON_FIELD_NUMBER: _ClassVar[int]
    question_id: str
    golden_set: str
    bucket: str
    asker: str
    question: str
    expected: GoldenExpected
    status: str
    origin: str
    submitted_by: str
    submitted_at: _timestamp_pb2.Timestamp
    expert_reviewer: str
    platform_reviewer: str
    reject_reason: str
    def __init__(self, question_id: _Optional[str] = ..., golden_set: _Optional[str] = ..., bucket: _Optional[str] = ..., asker: _Optional[str] = ..., question: _Optional[str] = ..., expected: _Optional[_Union[GoldenExpected, _Mapping]] = ..., status: _Optional[str] = ..., origin: _Optional[str] = ..., submitted_by: _Optional[str] = ..., submitted_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., expert_reviewer: _Optional[str] = ..., platform_reviewer: _Optional[str] = ..., reject_reason: _Optional[str] = ...) -> None: ...

class ListQuestionsRequest(_message.Message):
    __slots__ = ("tenant", "include_candidates")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_CANDIDATES_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    include_candidates: bool
    def __init__(self, tenant: _Optional[str] = ..., include_candidates: _Optional[bool] = ...) -> None: ...

class ListQuestionsResponse(_message.Message):
    __slots__ = ("questions", "golden_versions")
    QUESTIONS_FIELD_NUMBER: _ClassVar[int]
    GOLDEN_VERSIONS_FIELD_NUMBER: _ClassVar[int]
    questions: _containers.RepeatedCompositeFieldContainer[GoldenQuestion]
    golden_versions: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, questions: _Optional[_Iterable[_Union[GoldenQuestion, _Mapping]]] = ..., golden_versions: _Optional[_Iterable[str]] = ...) -> None: ...

class SubmitCandidateRequest(_message.Message):
    __slots__ = ("tenant", "golden_set", "bucket", "asker", "question", "expected", "origin")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    GOLDEN_SET_FIELD_NUMBER: _ClassVar[int]
    BUCKET_FIELD_NUMBER: _ClassVar[int]
    ASKER_FIELD_NUMBER: _ClassVar[int]
    QUESTION_FIELD_NUMBER: _ClassVar[int]
    EXPECTED_FIELD_NUMBER: _ClassVar[int]
    ORIGIN_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    golden_set: str
    bucket: str
    asker: str
    question: str
    expected: GoldenExpected
    origin: str
    def __init__(self, tenant: _Optional[str] = ..., golden_set: _Optional[str] = ..., bucket: _Optional[str] = ..., asker: _Optional[str] = ..., question: _Optional[str] = ..., expected: _Optional[_Union[GoldenExpected, _Mapping]] = ..., origin: _Optional[str] = ...) -> None: ...

class SubmitCandidateResponse(_message.Message):
    __slots__ = ("question",)
    QUESTION_FIELD_NUMBER: _ClassVar[int]
    question: GoldenQuestion
    def __init__(self, question: _Optional[_Union[GoldenQuestion, _Mapping]] = ...) -> None: ...

class ReviewCandidateRequest(_message.Message):
    __slots__ = ("tenant", "question_id", "approved", "reason")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    QUESTION_ID_FIELD_NUMBER: _ClassVar[int]
    APPROVED_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    question_id: str
    approved: bool
    reason: str
    def __init__(self, tenant: _Optional[str] = ..., question_id: _Optional[str] = ..., approved: _Optional[bool] = ..., reason: _Optional[str] = ...) -> None: ...

class ReviewCandidateResponse(_message.Message):
    __slots__ = ("question",)
    QUESTION_FIELD_NUMBER: _ClassVar[int]
    question: GoldenQuestion
    def __init__(self, question: _Optional[_Union[GoldenQuestion, _Mapping]] = ...) -> None: ...
