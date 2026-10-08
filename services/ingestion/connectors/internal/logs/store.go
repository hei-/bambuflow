package logs

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/redis/go-redis/v9"
)

// S3Store 把各文档已收到的消息存在对象存储里（已脱敏），一份文档一个对象。
type S3Store struct {
	Client *minio.Client
	Bucket string
	// 对象键前缀，如 state/src_applogs/
	Prefix string
}

func (s *S3Store) key(docID string) string { return s.Prefix + docID + ".json" }

func (s *S3Store) Load(ctx context.Context, docID string) (*DayState, error) {
	obj, err := s.Client.GetObject(ctx, s.Bucket, s.key(docID), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = obj.Close() }()
	raw, err := io.ReadAll(obj)
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return &DayState{}, nil
		}
		return nil, err
	}
	var st DayState
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

func (s *S3Store) Save(ctx context.Context, docID string, st *DayState) error {
	raw, err := json.Marshal(st)
	if err != nil {
		return err
	}
	_, err = s.Client.PutObject(ctx, s.Bucket, s.key(docID), bytes.NewReader(raw), int64(len(raw)),
		minio.PutObjectOptions{ContentType: "application/json"})
	return err
}

func (s *S3Store) Delete(ctx context.Context, docID string) error {
	return s.Client.RemoveObject(ctx, s.Bucket, s.key(docID), minio.RemoveObjectOptions{})
}

func (s *S3Store) List(ctx context.Context) ([]string, error) {
	var out []string
	for o := range s.Client.ListObjects(ctx, s.Bucket, minio.ListObjectsOptions{Prefix: s.Prefix}) {
		if o.Err != nil {
			return nil, o.Err
		}
		out = append(out, strings.TrimSuffix(strings.TrimPrefix(o.Key, s.Prefix), ".json"))
	}
	return out, nil
}

// RedisPublished 与其他样例源共用同一种同步状态：Redis 哈希 doc_id → “内容摘要|权限摘要”。
type RedisPublished struct {
	Client *redis.Client
	Key    string
}

func (r *RedisPublished) All(ctx context.Context) (map[string]string, error) {
	return r.Client.HGetAll(ctx, r.Key).Result()
}

func (r *RedisPublished) Set(ctx context.Context, docID, fp string) error {
	return r.Client.HSet(ctx, r.Key, docID, fp).Err()
}

func (r *RedisPublished) Del(ctx context.Context, docID string) error {
	return r.Client.HDel(ctx, r.Key, docID).Err()
}
