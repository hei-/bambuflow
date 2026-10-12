import datetime

from bambuflow.answer.v1 import answer_pb2 as _answer_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ListAnswerRecordsRequest(_message.Message):
    __slots__ = ("tenant", "experiment_id", "since", "until", "include_answer", "page_size", "page_token")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    EXPERIMENT_ID_FIELD_NUMBER: _ClassVar[int]
    SINCE_FIELD_NUMBER: _ClassVar[int]
    UNTIL_FIELD_NUMBER: _ClassVar[int]
    INCLUDE_ANSWER_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    experiment_id: str
    since: _timestamp_pb2.Timestamp
    until: _timestamp_pb2.Timestamp
    include_answer: bool
    page_size: int
    page_token: str
    def __init__(self, tenant: _Optional[str] = ..., experiment_id: _Optional[str] = ..., since: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., until: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., include_answer: _Optional[bool] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class ListAnswerRecordsResponse(_message.Message):
    __slots__ = ("records", "next_page_token")
    RECORDS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    records: _containers.RepeatedCompositeFieldContainer[AnswerRecord]
    next_page_token: str
    def __init__(self, records: _Optional[_Iterable[_Union[AnswerRecord, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class AnswerRecord(_message.Message):
    __slots__ = ("answer_id", "user_key", "created_at", "status", "experiment_id", "experiment_arm", "latency_ms", "usage", "feedback", "body_withheld", "answer", "masked_question", "feedback_comments", "coverage", "access_denied", "canary_releases")
    ANSWER_ID_FIELD_NUMBER: _ClassVar[int]
    USER_KEY_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    EXPERIMENT_ID_FIELD_NUMBER: _ClassVar[int]
    EXPERIMENT_ARM_FIELD_NUMBER: _ClassVar[int]
    LATENCY_MS_FIELD_NUMBER: _ClassVar[int]
    USAGE_FIELD_NUMBER: _ClassVar[int]
    FEEDBACK_FIELD_NUMBER: _ClassVar[int]
    BODY_WITHHELD_FIELD_NUMBER: _ClassVar[int]
    ANSWER_FIELD_NUMBER: _ClassVar[int]
    MASKED_QUESTION_FIELD_NUMBER: _ClassVar[int]
    FEEDBACK_COMMENTS_FIELD_NUMBER: _ClassVar[int]
    COVERAGE_FIELD_NUMBER: _ClassVar[int]
    ACCESS_DENIED_FIELD_NUMBER: _ClassVar[int]
    CANARY_RELEASES_FIELD_NUMBER: _ClassVar[int]
    answer_id: str
    user_key: str
    created_at: _timestamp_pb2.Timestamp
    status: _answer_pb2.Status
    experiment_id: str
    experiment_arm: _answer_pb2.ExperimentArm
    latency_ms: int
    usage: _answer_pb2.TokenUsage
    feedback: _containers.RepeatedScalarFieldContainer[str]
    body_withheld: bool
    answer: _answer_pb2.AnswerResponse
    masked_question: str
    feedback_comments: _containers.RepeatedScalarFieldContainer[str]
    coverage: float
    access_denied: int
    canary_releases: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, answer_id: _Optional[str] = ..., user_key: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., status: _Optional[_Union[_answer_pb2.Status, str]] = ..., experiment_id: _Optional[str] = ..., experiment_arm: _Optional[_Union[_answer_pb2.ExperimentArm, str]] = ..., latency_ms: _Optional[int] = ..., usage: _Optional[_Union[_answer_pb2.TokenUsage, _Mapping]] = ..., feedback: _Optional[_Iterable[str]] = ..., body_withheld: _Optional[bool] = ..., answer: _Optional[_Union[_answer_pb2.AnswerResponse, _Mapping]] = ..., masked_question: _Optional[str] = ..., feedback_comments: _Optional[_Iterable[str]] = ..., coverage: _Optional[float] = ..., access_denied: _Optional[int] = ..., canary_releases: _Optional[_Iterable[str]] = ...) -> None: ...
