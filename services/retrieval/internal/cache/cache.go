// Package cache 是 retrieval 的检索结果与查询向量缓存（详设第 8 节）的 Redis 实现，以及按 ChunkAclChanged 清缓存的消费者。
//
// 缓存用单独的 Redis（BF_CACHE_REDIS_ADDR），与事件流的 Redis 分开（入库管道详设第 12 节）；不配置时不缓存。
// 键：
//   - bf:rc:<结果键>：检索结果（JSON，不含正文），10 分钟；
//   - bf:rc:doc:<doc_id>：引用这份文档的结果键集合（反向索引），TTL 与结果相同；
//   - bf:rc:changed:<doc_id>：这份文档最近一次权限变化的时间（Unix 纳秒），1 小时，写结果前据此放弃过时的写入；
//   - bf:qv:<模型版本>:<查询文本摘要>：查询向量，7 天。
package cache

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/hei-/bambuflow/services/retrieval/internal/search"
)

const (
	ResultTTL  = 10 * time.Minute
	VectorTTL  = 7 * 24 * time.Hour
	ChangedTTL = time.Hour
	// 每次读写缓存的超时：缓存慢就当没有，不拖慢检索
	opTimeout = 200 * time.Millisecond
)

func resultKey(k string) string    { return "bf:rc:" + k }
func docKey(doc string) string     { return "bf:rc:doc:" + doc }
func changedKey(doc string) string { return "bf:rc:changed:" + doc }

// Results 是检索结果缓存。读写失败都当作没命中、没写入，只记日志。
type Results struct {
	Client redis.UniversalClient
	Log    *slog.Logger
}

func (r *Results) Get(ctx context.Context, key string) (*search.CachedResult, bool) {
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	b, err := r.Client.Get(ctx, resultKey(key)).Bytes()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			r.Log.WarnContext(ctx, "读检索结果缓存失败", slog.Any("err", err))
		}
		return nil, false
	}
	var out search.CachedResult
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, false
	}
	return &out, true
}

func (r *Results) Put(ctx context.Context, key string, res *search.CachedResult, docs []string, since time.Time) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), opTimeout)
	defer cancel()
	// 本次请求开始之后有文档变过权限：结果可能过时，不写（详设 8.3）
	if len(docs) > 0 {
		keys := make([]string, len(docs))
		for i, d := range docs {
			keys[i] = changedKey(d)
		}
		vals, err := r.Client.MGet(ctx, keys...).Result()
		if err != nil {
			r.Log.WarnContext(ctx, "读权限变化时间失败，不写缓存", slog.Any("err", err))
			return
		}
		for _, v := range vals {
			if s, ok := v.(string); ok {
				if ns, err := strconv.ParseInt(s, 10, 64); err == nil && ns >= since.UnixNano() {
					return
				}
			}
		}
	}
	b, err := json.Marshal(res)
	if err != nil {
		return
	}
	pipe := r.Client.TxPipeline()
	pipe.Set(ctx, resultKey(key), b, ResultTTL)
	for _, d := range docs {
		pipe.SAdd(ctx, docKey(d), key)
		pipe.Expire(ctx, docKey(d), ResultTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		r.Log.WarnContext(ctx, "写检索结果缓存失败", slog.Any("err", err))
	}
}

// Invalidate 清掉引用这些文档的结果，并记下变化时间。
func (r *Results) Invalidate(ctx context.Context, docs []string, at time.Time) error {
	for _, d := range docs {
		keys, err := r.Client.SMembers(ctx, docKey(d)).Result()
		if err != nil {
			return err
		}
		pipe := r.Client.TxPipeline()
		pipe.Set(ctx, changedKey(d), strconv.FormatInt(at.UnixNano(), 10), ChangedTTL)
		for _, k := range keys {
			pipe.Del(ctx, resultKey(k))
		}
		pipe.Del(ctx, docKey(d))
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Vectors 是查询向量缓存：进程内（最多 maxLocal 条，满了整个清空）加 Redis。稠密与稀疏向量一起存。
type Vectors struct {
	Client redis.UniversalClient
	Log    *slog.Logger

	mu    sync.Mutex
	local map[string]search.QueryVector
}

const maxLocal = 10000

func vectorKey(model, text string) string {
	sum := sha256.Sum256([]byte(text))
	return "bf:qv:" + model + ":" + hex.EncodeToString(sum[:])
}

// stored 是 Redis 里的一条：稠密向量按小端 float32 字节存（JSON 里是 base64），稀疏向量原样
type stored struct {
	Dense  []byte             `json:"d"`
	Sparse map[uint32]float32 `json:"s,omitempty"`
}

func (v *Vectors) Get(ctx context.Context, model string, texts []string) map[int]search.QueryVector {
	out := map[int]search.QueryVector{}
	var miss []int
	v.mu.Lock()
	for i, t := range texts {
		if vec, ok := v.local[vectorKey(model, t)]; ok {
			out[i] = vec
		} else {
			miss = append(miss, i)
		}
	}
	v.mu.Unlock()
	if len(miss) == 0 {
		return out
	}
	keys := make([]string, len(miss))
	for j, i := range miss {
		keys[j] = vectorKey(model, texts[i])
	}
	ctx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	vals, err := v.Client.MGet(ctx, keys...).Result()
	if err != nil {
		v.Log.WarnContext(ctx, "读查询向量缓存失败", slog.Any("err", err))
		return out
	}
	for j, val := range vals {
		raw, ok := val.(string)
		if !ok {
			continue
		}
		var st stored
		if json.Unmarshal([]byte(raw), &st) != nil {
			continue
		}
		dense, ok := decode(st.Dense)
		if !ok {
			continue
		}
		qv := search.QueryVector{Dense: dense, Sparse: st.Sparse}
		out[miss[j]] = qv
		v.remember(keys[j], qv)
	}
	return out
}

func (v *Vectors) Put(ctx context.Context, model string, texts []string, vecs []search.QueryVector) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), opTimeout)
	defer cancel()
	pipe := v.Client.Pipeline()
	for i, t := range texts {
		k := vectorKey(model, t)
		v.remember(k, vecs[i])
		b, err := json.Marshal(stored{Dense: encode(vecs[i].Dense), Sparse: vecs[i].Sparse})
		if err != nil {
			continue
		}
		pipe.Set(ctx, k, b, VectorTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		v.Log.WarnContext(ctx, "写查询向量缓存失败", slog.Any("err", err))
	}
}

func (v *Vectors) remember(k string, vec search.QueryVector) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.local == nil || len(v.local) >= maxLocal {
		v.local = map[string]search.QueryVector{}
	}
	v.local[k] = vec
}

// encode 把向量存成小端 float32 字节
func encode(vec []float32) []byte {
	b := make([]byte, 4*len(vec))
	for i, x := range vec {
		binary.LittleEndian.PutUint32(b[4*i:], math.Float32bits(x))
	}
	return b
}

func decode(b []byte) ([]float32, bool) {
	if len(b) == 0 || len(b)%4 != 0 {
		return nil, false
	}
	out := make([]float32, len(b)/4)
	for i := range out {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[4*i : 4*i+4]))
	}
	return out, true
}
