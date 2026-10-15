// Package revocation 是读副本下撤权不被旧副本绕过的两样共用状态（元数据库可用性详设第 4、5 节）：
//
//   - 撤权位点 W：indexing 每提交一次权限或删除变更，把 W 原子地抬到该事务的提交位点，只增不减；
//     retrieval 只采纳回放位点 r ≥ W 的副本读到的权限。
//   - 待生效撤权名单：indexing 收到权限或删除事件先登记，写入全部完成后移除；名单里的文档（与分区授权）
//     retrieval 一律按不可见处理，覆盖撤权完成之前、以及主库写不进时的窗口。
//
// 两者放在单独的 Redis 实例里，不与事件流共用：事件流实例故障切换可能丢消息，名单不能跟着丢（详设第 9 节第 3 项）。
// 位点只用数据库自己的复制位点比较，不比较节点时钟。
package revocation

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// FenceKey 存撤权位点 W，16 位十六进制（定长，Lua 里按字符串比较大小，不经浮点数）
	FenceKey = "bf:revocation:fence"
	// PendingKey 是待生效撤权名单里的文档（有序集合，成员见 DocMember），分数是到期时间（Unix 毫秒）
	PendingKey = "bf:revocation:pending"
	// PendingPartitionsKey 是名单里的分区（成员见 PartitionMember）。分区授权变化很少，读取时整个取回：
	// 读分区授权之前还不知道会涉及哪些分区
	PendingPartitionsKey = "bf:revocation:pending-partitions"
	// sinceKey 记每个条目第一次登记的时间（有序集合，只在第一次写入），续期不改：最老条目的年龄用它算（详设 7.2）
	sinceKey = "bf:revocation:pending-since"
	// PendingTTL 是名单条目的有效期；写入仍未完成时 indexing 重试前续期
	PendingTTL = 30 * time.Minute
)

// SQL 按 PostgreSQL 读位点（数据库适配器）。主库上回放位点取插入位点：它不小于任何已提交事务的提交位点，
// 单实例部署（读副本就是主库）时 r ≥ W 天然成立。
const (
	// ReplaySQL 读副本的回放位点 r
	ReplaySQL = `SELECT (CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_insert_lsn() END)::text`
	// ReceiveSQL 读同步备库的已接收位点（兜底条件 r ≥ S 的 S）
	ReceiveSQL = `SELECT (CASE WHEN pg_is_in_recovery() THEN pg_last_wal_receive_lsn() ELSE pg_current_wal_insert_lsn() END)::text`
	// CommitSQL 在主库上读当前插入位点：在事务提交返回之后读，不小于该事务的提交位点
	CommitSQL = `SELECT pg_current_wal_insert_lsn()::text`
	// LagSQL 读副本的回放延迟（秒）：只用于全量重建限速，不参与正确性判断（详设 7.1）；主库上为 0
	LagSQL = `SELECT CASE WHEN pg_is_in_recovery()
		THEN coalesce(extract(epoch FROM now() - pg_last_xact_replay_timestamp()), 0) ELSE 0 END::float8`
)

// LSN 是复制位点（PostgreSQL 的 pg_lsn）。
type LSN uint64

// ParseLSN 解析 "X/Y" 形式的位点。
func ParseLSN(s string) (LSN, error) {
	hi, lo, ok := strings.Cut(s, "/")
	if !ok {
		return 0, fmt.Errorf("复制位点格式不对: %q", s)
	}
	h, err := strconv.ParseUint(hi, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("复制位点格式不对: %q", s)
	}
	l, err := strconv.ParseUint(lo, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("复制位点格式不对: %q", s)
	}
	return LSN(h<<32 | l), nil
}

func (l LSN) String() string { return fmt.Sprintf("%X/%X", uint64(l)>>32, uint64(l)&0xFFFFFFFF) }

func (l LSN) key() string { return fmt.Sprintf("%016x", uint64(l)) }

// DocMember 是名单里的一份文档。
func DocMember(docID string) string { return "doc:" + docID }

// PartitionMember 是名单里的一个分区（分区授权变化，ADR-14）：名单期间这个分区按无人获授权处理。
// 与 retrieval 展开出的分区 token 同形（part:<数据源>/<分区>）。
func PartitionMember(sourceID, partition string) string { return "part:" + sourceID + "/" + partition }

func keyOf(member string) string {
	if strings.HasPrefix(member, "part:") {
		return PendingPartitionsKey
	}
	return PendingKey
}

// raise 把 W 抬到 max(W, ARGV[1])，返回抬之后的值
var raise = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if (not cur) or cur < ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[1])
  return ARGV[1]
end
return cur`)

// Store 读写撤权位点与名单。
type Store struct {
	Redis redis.Cmdable
	Now   func() time.Time
}

func (s *Store) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

// Raise 把撤权位点抬到不低于 p。
func (s *Store) Raise(ctx context.Context, p LSN) error {
	return raise.Run(ctx, s.Redis, []string{FenceKey}, p.key()).Err()
}

// Init 在撤权位点缺失时（Redis 故障切换丢了）用 p（主库当前位点）重建；已有时不动。返回是否重建了。
func (s *Store) Init(ctx context.Context, p LSN) (bool, error) {
	return s.Redis.SetNX(ctx, FenceKey, p.key(), 0).Result()
}

// Hold 把成员登记进名单（已有的续期），顺带清掉到期的条目。
func (s *Store) Hold(ctx context.Context, members ...string) error {
	if len(members) == 0 {
		return nil
	}
	now := s.now()
	until := float64(now.Add(PendingTTL).UnixMilli())
	byKey := map[string][]redis.Z{}
	since := make([]redis.Z, len(members))
	for i, m := range members {
		byKey[keyOf(m)] = append(byKey[keyOf(m)], redis.Z{Score: until, Member: m})
		since[i] = redis.Z{Score: float64(now.UnixMilli()), Member: m}
	}
	_, err := s.Redis.Pipelined(ctx, func(p redis.Pipeliner) error {
		p.ZAddNX(ctx, sinceKey, since...)
		for key, zs := range byKey {
			p.ZAdd(ctx, key, zs...)
			p.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(now.UnixMilli(), 10))
		}
		return nil
	})
	return err
}

// Release 把成员移出名单。
func (s *Store) Release(ctx context.Context, members ...string) error {
	byKey := map[string][]any{}
	for _, m := range members {
		byKey[keyOf(m)] = append(byKey[keyOf(m)], m)
	}
	if len(byKey) == 0 {
		return nil
	}
	_, err := s.Redis.Pipelined(ctx, func(p redis.Pipeliner) error {
		for key, ms := range byKey {
			p.ZRem(ctx, key, ms...)
			p.ZRem(ctx, sinceKey, ms...)
		}
		return nil
	})
	return err
}

// Stats 返回名单里仍有效的条目数与最老条目从第一次登记起的时长。到期没移出的条目（写入方停了）顺带从登记时间里清掉。
func (s *Store) Stats(ctx context.Context) (int64, time.Duration, error) {
	now := s.now()
	lo := "(" + strconv.FormatInt(now.UnixMilli(), 10)
	var n int64
	for _, key := range []string{PendingKey, PendingPartitionsKey} {
		c, err := s.Redis.ZCount(ctx, key, lo, "+inf").Result()
		if err != nil {
			return 0, 0, err
		}
		n += c
	}
	for range 100 {
		first, err := s.Redis.ZRangeWithScores(ctx, sinceKey, 0, 0).Result()
		if err != nil || len(first) == 0 {
			return n, 0, err
		}
		m, _ := first[0].Member.(string)
		until, err := s.Redis.ZScore(ctx, keyOf(m), m).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return n, 0, err
		}
		if until > float64(now.UnixMilli()) {
			return n, now.Sub(time.UnixMilli(int64(first[0].Score))), nil
		}
		if err := s.Redis.ZRem(ctx, sinceKey, m).Err(); err != nil {
			return n, 0, err
		}
	}
	return n, 0, nil
}

// ErrNoFence 表示撤权位点缺失：retrieval 改用兜底条件，直到 indexing 重建它。
var ErrNoFence = errors.New("撤权位点缺失")

// View 是一次读取前从 Redis 取到的状态：撤权位点、所问文档里仍在名单中的那些，以及名单里的全部分区。
type View struct {
	Fence LSN
	Held  map[string]bool
}

// Read 在同一次往返里读撤权位点、docs（DocMember）在名单中的情况与名单里的全部分区
// （详设 4.1：W 与名单同时读，且先于读权限数据）。撤权位点缺失时返回 ErrNoFence，Held 仍然有效。
func (s *Store) Read(ctx context.Context, docs []string) (View, error) {
	var fence *redis.StringCmd
	var scores *redis.FloatSliceCmd
	var parts *redis.StringSliceCmd
	now := s.now().UnixMilli()
	_, err := s.Redis.Pipelined(ctx, func(p redis.Pipeliner) error {
		fence = p.Get(ctx, FenceKey)
		if len(docs) > 0 {
			scores = p.ZMScore(ctx, PendingKey, docs...)
		}
		parts = p.ZRangeByScore(ctx, PendingPartitionsKey, &redis.ZRangeBy{Min: "(" + strconv.FormatInt(now, 10), Max: "+inf"})
		return nil
	})
	if err != nil && !errors.Is(err, redis.Nil) {
		return View{}, err
	}
	v := View{Held: map[string]bool{}}
	if scores != nil {
		for i, sc := range scores.Val() {
			// ZMSCORE 对不存在的成员给 0
			if sc > float64(now) {
				v.Held[docs[i]] = true
			}
		}
	}
	for _, m := range parts.Val() {
		v.Held[m] = true
	}
	raw, err := fence.Result()
	if errors.Is(err, redis.Nil) {
		return v, ErrNoFence
	}
	if err != nil {
		return View{}, err
	}
	n, err := strconv.ParseUint(raw, 16, 64)
	if err != nil {
		return View{}, fmt.Errorf("撤权位点格式不对: %q", raw)
	}
	v.Fence = LSN(n)
	return v, nil
}
