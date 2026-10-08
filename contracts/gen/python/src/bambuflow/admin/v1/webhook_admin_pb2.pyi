import datetime

from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DeliveryStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DELIVERY_STATUS_UNSPECIFIED: _ClassVar[DeliveryStatus]
    DELIVERY_STATUS_PENDING: _ClassVar[DeliveryStatus]
    DELIVERY_STATUS_DELIVERED: _ClassVar[DeliveryStatus]
    DELIVERY_STATUS_FAILED: _ClassVar[DeliveryStatus]
DELIVERY_STATUS_UNSPECIFIED: DeliveryStatus
DELIVERY_STATUS_PENDING: DeliveryStatus
DELIVERY_STATUS_DELIVERED: DeliveryStatus
DELIVERY_STATUS_FAILED: DeliveryStatus

class CreateSubscriptionRequest(_message.Message):
    __slots__ = ("tenant", "url", "event_types", "description")
    TENANT_FIELD_NUMBER: _ClassVar[int]
    URL_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPES_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    url: str
    event_types: _containers.RepeatedScalarFieldContainer[str]
    description: str
    def __init__(self, tenant: _Optional[str] = ..., url: _Optional[str] = ..., event_types: _Optional[_Iterable[str]] = ..., description: _Optional[str] = ...) -> None: ...

class CreateSubscriptionResponse(_message.Message):
    __slots__ = ("subscription", "signing_secret")
    SUBSCRIPTION_FIELD_NUMBER: _ClassVar[int]
    SIGNING_SECRET_FIELD_NUMBER: _ClassVar[int]
    subscription: WebhookSubscription
    signing_secret: str
    def __init__(self, subscription: _Optional[_Union[WebhookSubscription, _Mapping]] = ..., signing_secret: _Optional[str] = ...) -> None: ...

class ListSubscriptionsRequest(_message.Message):
    __slots__ = ("tenant",)
    TENANT_FIELD_NUMBER: _ClassVar[int]
    tenant: str
    def __init__(self, tenant: _Optional[str] = ...) -> None: ...

class ListSubscriptionsResponse(_message.Message):
    __slots__ = ("subscriptions",)
    SUBSCRIPTIONS_FIELD_NUMBER: _ClassVar[int]
    subscriptions: _containers.RepeatedCompositeFieldContainer[WebhookSubscription]
    def __init__(self, subscriptions: _Optional[_Iterable[_Union[WebhookSubscription, _Mapping]]] = ...) -> None: ...

class DeleteSubscriptionRequest(_message.Message):
    __slots__ = ("subscription_id",)
    SUBSCRIPTION_ID_FIELD_NUMBER: _ClassVar[int]
    subscription_id: str
    def __init__(self, subscription_id: _Optional[str] = ...) -> None: ...

class DeleteSubscriptionResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ListDeliveriesRequest(_message.Message):
    __slots__ = ("subscription_id", "limit")
    SUBSCRIPTION_ID_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    subscription_id: str
    limit: int
    def __init__(self, subscription_id: _Optional[str] = ..., limit: _Optional[int] = ...) -> None: ...

class ListDeliveriesResponse(_message.Message):
    __slots__ = ("deliveries",)
    DELIVERIES_FIELD_NUMBER: _ClassVar[int]
    deliveries: _containers.RepeatedCompositeFieldContainer[WebhookDelivery]
    def __init__(self, deliveries: _Optional[_Iterable[_Union[WebhookDelivery, _Mapping]]] = ...) -> None: ...

class RedeliverRequest(_message.Message):
    __slots__ = ("subscription_id", "event_id")
    SUBSCRIPTION_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    subscription_id: str
    event_id: str
    def __init__(self, subscription_id: _Optional[str] = ..., event_id: _Optional[str] = ...) -> None: ...

class RedeliverResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class WebhookSubscription(_message.Message):
    __slots__ = ("subscription_id", "tenant", "url", "event_types", "description", "active", "created_at")
    SUBSCRIPTION_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_FIELD_NUMBER: _ClassVar[int]
    URL_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPES_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    ACTIVE_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    subscription_id: str
    tenant: str
    url: str
    event_types: _containers.RepeatedScalarFieldContainer[str]
    description: str
    active: bool
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, subscription_id: _Optional[str] = ..., tenant: _Optional[str] = ..., url: _Optional[str] = ..., event_types: _Optional[_Iterable[str]] = ..., description: _Optional[str] = ..., active: _Optional[bool] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class WebhookDelivery(_message.Message):
    __slots__ = ("event_id", "event_type", "status", "attempts", "last_status_code", "last_error", "created_at", "next_attempt_at", "delivered_at")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    LAST_STATUS_CODE_FIELD_NUMBER: _ClassVar[int]
    LAST_ERROR_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    NEXT_ATTEMPT_AT_FIELD_NUMBER: _ClassVar[int]
    DELIVERED_AT_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    event_type: str
    status: DeliveryStatus
    attempts: int
    last_status_code: int
    last_error: str
    created_at: _timestamp_pb2.Timestamp
    next_attempt_at: _timestamp_pb2.Timestamp
    delivered_at: _timestamp_pb2.Timestamp
    def __init__(self, event_id: _Optional[str] = ..., event_type: _Optional[str] = ..., status: _Optional[_Union[DeliveryStatus, str]] = ..., attempts: _Optional[int] = ..., last_status_code: _Optional[int] = ..., last_error: _Optional[str] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., next_attempt_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., delivered_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
