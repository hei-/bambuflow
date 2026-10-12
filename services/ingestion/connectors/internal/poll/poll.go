// Package poll 是常驻的清单轮询（入库管道详设第 5 节）：按间隔跑一轮同步，间隔按源系统的响应在 Min 与 Max 之间自动调整，
// 源系统的 Webhook 或管理员的立即同步可以提前唤醒一轮。
//
// 间隔怎么调：一轮被源系统限流（同步返回的错误里有 ErrThrottled）或耗时超过 Slow，间隔翻倍，最长 Max；
// 正常完成则每轮缩回 Step，最短 Min；其他失败不改间隔，下一轮照常重试。当前间隔写进同步状态，
// 计入这个源的撤权时效并在 console 的数据源页展示。
//
// 唤醒只是“现在来跑一轮”，不带变更内容：Webhook 的通知不可全信，变更仍以这一轮从源系统拉到的清单为准，
// 删除保护照常生效。两轮之间至少隔 MinGap，突发的大量通知合并成一轮。
package poll

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// ErrThrottled 表示源系统限流（如 HTTP 429、503 带 Retry-After）：连接器把它包进同步错误，轮询据此拉长间隔。
var ErrThrottled = errors.New("源系统限流")

// Pacer 按一轮的结果算下一轮的间隔。
type Pacer struct {
	Min, Max time.Duration
	// Slow：一轮耗时超过它视为源系统压力大，按限流处理
	Slow time.Duration
	// Step：正常完成后每轮缩回的量
	Step time.Duration
}

// DefaultPacer 是建议值：5 分钟起，最长 15 分钟（入库管道详设第 5 节）。
var DefaultPacer = Pacer{Min: 5 * time.Minute, Max: 15 * time.Minute, Slow: 2 * time.Minute, Step: time.Minute}

// Next 返回下一轮的间隔。
func (p Pacer) Next(cur, took time.Duration, err error) time.Duration {
	switch {
	case errors.Is(err, ErrThrottled) || took > p.Slow:
		return min(max(cur*2, p.Min), p.Max)
	case err != nil:
		return cur
	default:
		return max(cur-p.Step, p.Min)
	}
}

// Waker 等到下一轮的时刻或被唤醒。
type Waker interface {
	// Wait 最多等 d；被唤醒时提前返回 true。ctx 结束时返回 ctx 的错误
	Wait(ctx context.Context, d time.Duration) (bool, error)
}

// Status 记下当前间隔与下一轮的时间，供同步状态展示；也作为轮询进程还活着的心跳。
type Status interface {
	SetPolling(ctx context.Context, interval time.Duration, next time.Time) error
}

// MinGap：两轮之间的最短间隔，被唤醒时也不早于它
const MinGap = 10 * time.Second

type Loop struct {
	Pacer  Pacer
	Round  func(ctx context.Context) error
	Waker  Waker
	Status Status
	Log    *slog.Logger
	Now    func() time.Time
	// Gap 是两轮之间的最短间隔，为零时取 MinGap
	Gap time.Duration
}

// Run 一轮接一轮地跑，直到 ctx 结束。
func (l *Loop) Run(ctx context.Context) error {
	interval := l.Pacer.Min
	for {
		start := l.Now()
		err := l.Round(ctx)
		if ctx.Err() != nil {
			return nil
		}
		took := l.Now().Sub(start)
		next := l.Pacer.Next(interval, took, err)
		if next != interval {
			l.Log.InfoContext(ctx, "调整清单轮询间隔", slog.Duration("from", interval), slog.Duration("to", next),
				slog.Duration("took", took), slog.Bool("throttled", errors.Is(err, ErrThrottled)))
		}
		interval = next
		if err != nil {
			l.Log.ErrorContext(ctx, "本轮同步失败，下一轮重试", slog.Any("err", err))
		}
		if err := l.Status.SetPolling(ctx, interval, l.Now().Add(interval)); err != nil {
			l.Log.WarnContext(ctx, "写轮询状态失败", slog.Any("err", err))
		}
		if err := l.wait(ctx, interval); err != nil {
			return nil
		}
	}
}

// wait 先无条件等最短间隔（期间的唤醒留到之后处理），再等到间隔结束或被唤醒。
func (l *Loop) wait(ctx context.Context, interval time.Duration) error {
	gap := MinGap
	if l.Gap > 0 {
		gap = l.Gap
	}
	gap = min(gap, interval)
	t := time.NewTimer(gap)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
	}
	_, err := l.Waker.Wait(ctx, interval-gap)
	return err
}
