package webhook

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	eventsv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/events/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ErrNotFound：订阅或投递记录不存在。
var ErrNotFound = errors.New("不存在")

// Subscription 是一个订阅；SecretPath 是签名密钥在 Vault 里的路径。
type Subscription struct {
	ID, Tenant, URL, Description, SecretPath string
	EventTypes                               []string
	Active                                   bool
	CreatedAt                                time.Time
}

func (s Subscription) proto() *adminv1.WebhookSubscription {
	return &adminv1.WebhookSubscription{
		SubscriptionId: s.ID, Tenant: s.Tenant, Url: s.URL, EventTypes: s.EventTypes, Description: s.Description,
		Active: s.Active, CreatedAt: timestamppb.New(s.CreatedAt),
	}
}

// Delivery 是领取到的一次投递。
type Delivery struct {
	EventID, SubscriptionID string
	Payload                 []byte
	Attempts                int
	WindowStart             time.Time
	URL, SecretPath         string
	Active                  bool
}

type PG struct {
	Pool *pgxpool.Pool
}

func (s *PG) CreateSubscription(ctx context.Context, sub Subscription) (Subscription, error) {
	err := s.Pool.QueryRow(ctx, `
		INSERT INTO governance.webhook_subscriptions (subscription_id, tenant, url, event_types, description, secret_path)
		VALUES ($1, $2, $3, $4, $5, $6) RETURNING active, created_at`,
		sub.ID, sub.Tenant, sub.URL, sub.EventTypes, sub.Description, sub.SecretPath).Scan(&sub.Active, &sub.CreatedAt)
	return sub, err
}

func (s *PG) ListSubscriptions(ctx context.Context, tenant string) ([]Subscription, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT subscription_id, tenant, url, event_types, description, secret_path, active, created_at
		FROM governance.webhook_subscriptions WHERE $1 = '' OR tenant = $1 ORDER BY created_at`, tenant)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Subscription, error) {
		var x Subscription
		err := r.Scan(&x.ID, &x.Tenant, &x.URL, &x.EventTypes, &x.Description, &x.SecretPath, &x.Active, &x.CreatedAt)
		return x, err
	})
}

// Deactivate 停用订阅，并把它还没送达的投递标为失败。
func (s *PG) Deactivate(ctx context.Context, id string) error {
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE governance.webhook_subscriptions SET active = false WHERE subscription_id = $1`, id)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		_, err = tx.Exec(ctx, `
			UPDATE governance.webhook_deliveries SET status = 'failed', last_error = '订阅已停用', lease_until = NULL
			WHERE subscription_id = $1 AND status = 'pending'`, id)
		return err
	})
}

// Enqueue 为匹配的有效订阅各记一条待投递，返回新增的条数。重复的事件不会新增（主键去重）。
func (s *PG) Enqueue(ctx context.Context, evt *eventsv1.PlatformEvent) (int64, error) {
	payload, err := Payload(evt)
	if err != nil {
		return 0, err
	}
	rows, err := s.Pool.Query(ctx, `
		SELECT subscription_id, tenant, event_types FROM governance.webhook_subscriptions
		WHERE active AND ($1 = '' OR tenant = $1) AND $2 = ANY(event_types)`, evt.GetTenant(), evt.GetType())
	if err != nil {
		return 0, err
	}
	type sub struct {
		id, tenant string
		types      []string
	}
	subs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (sub, error) {
		var x sub
		err := r.Scan(&x.id, &x.tenant, &x.types)
		return x, err
	})
	if err != nil {
		return 0, err
	}
	var n int64
	for _, x := range subs {
		// 查询已按同样的条件筛过；这里再按 Matches 判一次，匹配规则只有一处定义
		if !Matches(x.tenant, x.types, evt) {
			continue
		}
		tag, err := s.Pool.Exec(ctx, `
			INSERT INTO governance.webhook_deliveries (event_id, subscription_id, event_type, payload, status)
			VALUES ($1, $2, $3, $4, 'pending') ON CONFLICT DO NOTHING`, evt.GetEventId(), x.id, evt.GetType(), payload)
		if err != nil {
			return n, err
		}
		n += tag.RowsAffected()
	}
	return n, nil
}

// Claim 领取至多 n 条到期的投递，加 lease 的租约（实例中途退出时，过期后由别的实例接手）。
func (s *PG) Claim(ctx context.Context, n int, lease time.Duration) ([]Delivery, error) {
	rows, err := s.Pool.Query(ctx, `
		UPDATE governance.webhook_deliveries d
		SET attempts = d.attempts + 1, lease_until = now() + make_interval(secs => $2)
		FROM governance.webhook_subscriptions s
		WHERE s.subscription_id = d.subscription_id AND (d.event_id, d.subscription_id) IN (
			SELECT event_id, subscription_id FROM governance.webhook_deliveries
			WHERE status = 'pending' AND next_attempt_at <= now() AND (lease_until IS NULL OR lease_until < now())
			ORDER BY next_attempt_at LIMIT $1
			FOR UPDATE SKIP LOCKED)
		RETURNING d.event_id, d.subscription_id, d.payload, d.attempts, d.window_start, s.url, s.secret_path, s.active`,
		n, lease.Seconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Delivery, error) {
		var x Delivery
		err := r.Scan(&x.EventID, &x.SubscriptionID, &x.Payload, &x.Attempts, &x.WindowStart, &x.URL, &x.SecretPath, &x.Active)
		return x, err
	})
}

// Delivered 记下送达。
func (s *PG) Delivered(ctx context.Context, d Delivery, code int) error {
	_, err := s.Pool.Exec(ctx, `
		UPDATE governance.webhook_deliveries
		SET status = 'delivered', delivered_at = now(), last_status_code = $3, last_error = '', lease_until = NULL
		WHERE event_id = $1 AND subscription_id = $2`, d.EventID, d.SubscriptionID, code)
	return err
}

// Failed 记下一次失败：next 为零值时不再重试（status = failed），否则在 next 再投。
func (s *PG) Failed(ctx context.Context, d Delivery, code int, msg string, next time.Time) error {
	status, at := "pending", next
	if next.IsZero() {
		status, at = "failed", time.Now()
	}
	_, err := s.Pool.Exec(ctx, `
		UPDATE governance.webhook_deliveries
		SET status = $3, next_attempt_at = $4, last_status_code = $5, last_error = $6, lease_until = NULL
		WHERE event_id = $1 AND subscription_id = $2`, d.EventID, d.SubscriptionID, status, at, code, msg)
	return err
}

// Redeliver 手工重放：不论之前的结果，重新排队并开始新的 24 小时窗口。
func (s *PG) Redeliver(ctx context.Context, subscriptionID, eventID string) error {
	tag, err := s.Pool.Exec(ctx, `
		UPDATE governance.webhook_deliveries d
		SET status = 'pending', attempts = 0, next_attempt_at = now(), window_start = now(), lease_until = NULL, delivered_at = NULL
		FROM governance.webhook_subscriptions s
		WHERE s.subscription_id = d.subscription_id AND s.active AND d.subscription_id = $1 AND d.event_id = $2`,
		subscriptionID, eventID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PG) ListDeliveries(ctx context.Context, subscriptionID string, limit int) ([]*adminv1.WebhookDelivery, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT event_id, event_type, status, attempts, last_status_code, last_error, created_at, next_attempt_at, delivered_at
		FROM governance.webhook_deliveries WHERE subscription_id = $1 ORDER BY created_at DESC LIMIT $2`, subscriptionID, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (*adminv1.WebhookDelivery, error) {
		var (
			x             adminv1.WebhookDelivery
			status        string
			code          int32
			attempts      int32
			created, next time.Time
			delivered     *time.Time
		)
		if err := r.Scan(&x.EventId, &x.EventType, &status, &attempts, &code, &x.LastError, &created, &next, &delivered); err != nil {
			return nil, err
		}
		x.Status, x.Attempts, x.LastStatusCode = deliveryStatus(status), attempts, code
		x.CreatedAt = timestamppb.New(created)
		if status == "pending" {
			x.NextAttemptAt = timestamppb.New(next)
		}
		if delivered != nil {
			x.DeliveredAt = timestamppb.New(*delivered)
		}
		return &x, nil
	})
}

func deliveryStatus(s string) adminv1.DeliveryStatus {
	switch s {
	case "pending":
		return adminv1.DeliveryStatus_DELIVERY_STATUS_PENDING
	case "delivered":
		return adminv1.DeliveryStatus_DELIVERY_STATUS_DELIVERED
	case "failed":
		return adminv1.DeliveryStatus_DELIVERY_STATUS_FAILED
	}
	return adminv1.DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED
}
