package audit

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// Store 是只追加的审计存储（技术设计文档 8.3）：同一个键不覆盖，生产用开启对象锁的桶。
type Store interface {
	Put(ctx context.Context, key string, body []byte) error
}

// Shipper 定期把当前段封存并送往 Store。送达后才删本地段，失败的段留在本地下一轮重试，
// 所以本地卷就是持久队列：gateway 重启或存储不可用都不丢记录。
type Shipper struct {
	W     *Writer
	Store Store
	// Instance 区分 gateway 实例。段的对象键是 gateway/<送达日期>/<实例>/<段名>（UTC 日期）：
	// governance 的审计索引任务只列最近两天的日期目录（governance internal/auditquery）
	Instance string
	Log      *slog.Logger
}

// Ship 执行一轮：封存当前段，按时间顺序上传所有封存段；返回送出的段数。遇到第一个失败就停，保持顺序。
func (s *Shipper) Ship(ctx context.Context, now time.Time) (int, error) {
	if _, err := s.W.Rotate(now); err != nil {
		return 0, fmt.Errorf("审计切段: %w", err)
	}
	segs, err := s.W.Sealed()
	if err != nil {
		return 0, err
	}
	for i, seg := range segs {
		body, err := os.ReadFile(filepath.Clean(seg))
		if err != nil {
			return i, err
		}
		key := fmt.Sprintf("gateway/%s/%s/%s", now.UTC().Format("2006/01/02"), s.Instance, filepath.Base(seg))
		if err := s.Store.Put(ctx, key, body); err != nil {
			return i, fmt.Errorf("上传审计段 %s: %w", filepath.Base(seg), err)
		}
		if err := os.Remove(seg); err != nil {
			return i, err
		}
	}
	return len(segs), nil
}

// Run 每隔 every 执行一轮，直到 ctx 结束；退出前再送一轮。
func (s *Shipper) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			// 退出时尽量把最后一段送走；送不出去的留在卷上，下次启动再送
			flush, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			s.once(flush)
			return
		case <-t.C:
			s.once(ctx)
		}
	}
}

func (s *Shipper) once(ctx context.Context) {
	if _, err := s.Ship(ctx, time.Now()); err != nil {
		s.Log.ErrorContext(ctx, "审计送出失败，下一轮重试", slog.Any("err", err))
	}
}
