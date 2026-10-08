// seed-logs 向日志主题（开发默认实现是 Redis Streams）写入一批确定的测试日志，只用于开发与 CI（P3-I3）。
//
// 消息覆盖日志数据源要验证的情况：配置过的服务与级别（payments-svc 的 ERROR、WARN，core-batch 的 ERROR）、
// 不入库的级别（INFO、DEBUG）与服务（marketing-web）、要脱敏的手机号、过了 30 天保留期的旧消息。
// 时间取运行当天（UTC）的前两天和 40 天前，消息 ID 由时间确定（毫秒-序号）：同一天重复运行时 Redis 拒绝不大于
// 已有最大 ID 的写入，这里把它当作已经写过，所以重复运行是幂等的（make ingest 会连跑两次）。
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/bambu-ai/bambuflow/libs/platform-go/logging"
)

type entry struct {
	at                   time.Time
	service, level, text string
}

// messages 生成某一天的测试日志，时间都在 08:00–08:15（UTC）之间，覆盖三个 5 分钟窗口。
func messages(day time.Time) []entry {
	at := func(m, s int) time.Time {
		return day.Add(8*time.Hour + time.Duration(m)*time.Minute + time.Duration(s)*time.Second)
	}
	var out []entry
	for i := range 6 {
		// 手机号是虚构的测试数据，入库前应被脱敏
		out = append(out, entry{at(i*2, 5), "payments-svc", "ERROR",
			fmt.Sprintf("payment timeout order=ORD%06d user=1381234%04d after %dms", 100+i, 5000+i, 3000+100*i)})
		out = append(out, entry{at(i*2, 20), "payments-svc", "WARN", fmt.Sprintf("channel unionpay retry %d latency %dms", i%3+1, 800+i)})
		out = append(out, entry{at(i*2, 35), "payments-svc", "INFO", "request served path=/pay status=200"})
		out = append(out, entry{at(i*2, 40), "payments-svc", "DEBUG", "debug dump of request headers"})
		out = append(out, entry{at(i*2+1, 10), "core-batch", "ERROR", fmt.Sprintf("batch job settle-%s failed step=%d code=E%d", day.Format("20060102"), i+1, 1040+i)})
		out = append(out, entry{at(i*2+1, 30), "marketing-web", "ERROR", fmt.Sprintf("campaign render failed id=%d", 70+i)})
	}
	return out
}

func main() {
	log := logging.New(os.Stdout, "seed-logs", slog.LevelInfo)
	addr, topic := os.Getenv("BF_REDIS_ADDR"), os.Getenv("BF_LOG_TOPIC")
	if addr == "" || topic == "" {
		log.Error("需要 BF_REDIS_ADDR 与 BF_LOG_TOPIC")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = rdb.Close() }()
	today := time.Now().UTC().Truncate(24 * time.Hour)
	var all []entry
	// 按时间先后写入：Redis Streams 的 ID 必须递增
	for _, d := range []int{-40, -2, -1} {
		all = append(all, messages(today.AddDate(0, 0, d))...)
	}
	added, skipped := 0, 0
	for _, e := range all {
		id := fmt.Sprintf("%d-0", e.at.UnixMilli())
		err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: topic, ID: id, Values: map[string]any{
			"service": e.service, "level": e.level, "ts": e.at.Format(time.RFC3339Nano), "msg": e.text,
		}}).Err()
		switch {
		case err == nil:
			added++
		case strings.Contains(err.Error(), "equal or smaller"):
			// 已经写过（重复运行），或流里已有更新的消息
			skipped++
		default:
			log.Error("写入日志主题失败", slog.Any("err", err))
			os.Exit(1)
		}
	}
	log.Info("测试日志已写入", slog.String("topic", topic), slog.Int("added", added), slog.Int("skipped", skipped))
}
