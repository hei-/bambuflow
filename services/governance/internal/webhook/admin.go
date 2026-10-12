package webhook

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
)

// AdminStore 是订阅管理用到的读写（PG）。
type AdminStore interface {
	CreateSubscription(ctx context.Context, s Subscription) (Subscription, error)
	ListSubscriptions(ctx context.Context, tenant string) ([]Subscription, error)
	Deactivate(ctx context.Context, id string) error
	ListDeliveries(ctx context.Context, subscriptionID string, limit int) ([]*adminv1.WebhookDelivery, error)
	Redeliver(ctx context.Context, subscriptionID, eventID string) error
}

// SecretWriter 把签名密钥写进 Vault KV。
type SecretWriter interface {
	WriteKV(ctx context.Context, mount, path string, data map[string]string) error
}

var (
	tenantPattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
	subIDPattern  = regexp.MustCompile(`^whk_[a-f0-9]+$`)
)

// Admin 实现 WebhookAdminService。现由命令行与端到端测试在内部网络调用（与 IndexAdminService 相同）；
// console 接入后经 gateway 的 /v1/admin 路由调用，由 gateway 做 RBAC 与管理审计（gateway 详设 4.2）。
type Admin struct {
	adminv1.UnimplementedWebhookAdminServiceServer
	Store   AdminStore
	Secrets SecretWriter
	Mount   string
	// 开发环境允许 http 回调地址
	AllowHTTP bool
	Log       *slog.Logger
}

func (a *Admin) CreateSubscription(ctx context.Context, r *adminv1.CreateSubscriptionRequest) (*adminv1.CreateSubscriptionResponse, error) {
	if !tenantPattern.MatchString(r.GetTenant()) || utf8.RuneCountInString(r.GetDescription()) > 200 {
		return nil, status.Error(codes.InvalidArgument, "tenant 不合法或说明过长")
	}
	if err := ValidURL(r.GetUrl(), a.AllowHTTP); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := ValidTypes(r.GetEventTypes()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	id, secret := NewSubscriptionID(), NewSecret()
	path := secretPath(id)
	// 先存密钥再登记订阅：订阅存在时密钥一定取得到
	if err := a.Secrets.WriteKV(ctx, a.Mount, path, map[string]string{"current": secret}); err != nil {
		a.Log.ErrorContext(ctx, "写入签名密钥失败", slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "暂时不可用")
	}
	sub, err := a.Store.CreateSubscription(ctx, Subscription{
		ID: id, Tenant: r.GetTenant(), URL: r.GetUrl(), EventTypes: r.GetEventTypes(), Description: r.GetDescription(), SecretPath: path,
	})
	if err != nil {
		a.Log.ErrorContext(ctx, "登记订阅失败", slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "暂时不可用")
	}
	a.Log.InfoContext(ctx, "已登记 Webhook 订阅", slog.String("subscription_id", id), slog.String("tenant", sub.Tenant),
		slog.Any("event_types", sub.EventTypes))
	return &adminv1.CreateSubscriptionResponse{Subscription: sub.proto(), SigningSecret: secret}, nil
}

func (a *Admin) ListSubscriptions(ctx context.Context, r *adminv1.ListSubscriptionsRequest) (*adminv1.ListSubscriptionsResponse, error) {
	subs, err := a.Store.ListSubscriptions(ctx, r.GetTenant())
	if err != nil {
		a.Log.ErrorContext(ctx, "读取订阅失败", slog.Any("err", err))
		return nil, status.Error(codes.Unavailable, "暂时不可用")
	}
	out := &adminv1.ListSubscriptionsResponse{}
	for _, s := range subs {
		out.Subscriptions = append(out.Subscriptions, s.proto())
	}
	return out, nil
}

func (a *Admin) DeleteSubscription(ctx context.Context, r *adminv1.DeleteSubscriptionRequest) (*adminv1.DeleteSubscriptionResponse, error) {
	if !subIDPattern.MatchString(r.GetSubscriptionId()) {
		return nil, status.Error(codes.NotFound, "订阅不存在")
	}
	if err := a.Store.Deactivate(ctx, r.GetSubscriptionId()); err != nil {
		return nil, a.storeError(ctx, err)
	}
	a.Log.InfoContext(ctx, "已停用 Webhook 订阅", slog.String("subscription_id", r.GetSubscriptionId()))
	return &adminv1.DeleteSubscriptionResponse{}, nil
}

func (a *Admin) ListDeliveries(ctx context.Context, r *adminv1.ListDeliveriesRequest) (*adminv1.ListDeliveriesResponse, error) {
	limit := int(r.GetLimit())
	if limit == 0 {
		limit = 50
	}
	if limit < 1 || limit > 200 {
		return nil, status.Error(codes.InvalidArgument, "limit 应为 1–200")
	}
	ds, err := a.Store.ListDeliveries(ctx, r.GetSubscriptionId(), limit)
	if err != nil {
		return nil, a.storeError(ctx, err)
	}
	return &adminv1.ListDeliveriesResponse{Deliveries: ds}, nil
}

func (a *Admin) Redeliver(ctx context.Context, r *adminv1.RedeliverRequest) (*adminv1.RedeliverResponse, error) {
	if err := a.Store.Redeliver(ctx, r.GetSubscriptionId(), r.GetEventId()); err != nil {
		return nil, a.storeError(ctx, err)
	}
	a.Log.InfoContext(ctx, "手工重放 Webhook", slog.String("subscription_id", r.GetSubscriptionId()), slog.String("event_id", r.GetEventId()))
	return &adminv1.RedeliverResponse{}, nil
}

// secretPath 是订阅的签名密钥在 Vault KV 里的路径。
func secretPath(subscriptionID string) string { return "webhooks/" + subscriptionID }

func (a *Admin) storeError(ctx context.Context, err error) error {
	if errors.Is(err, ErrNotFound) {
		return status.Error(codes.NotFound, "不存在")
	}
	a.Log.ErrorContext(ctx, "读写订阅失败", slog.Any("err", err))
	return status.Error(codes.Unavailable, "暂时不可用")
}
