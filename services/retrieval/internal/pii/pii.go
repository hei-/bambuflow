// Package pii 为允许看 PII 原文的调用者还原入库时掩码的原值（技术设计文档 8.6，入库管道详设第 6 节）。
//
// 索引里的正文只有等长掩码；原值按块用文档级数据密钥 AES-256-GCM 加密（附加数据为 chunk_id），
// 数据密钥由主密钥包裹。这里经密钥服务（端口 KeyService，默认 Vault transit）解开数据密钥、解密并按位置回填。
// 解开的数据密钥在内存里短时缓存（按包裹后的形式为键），同一文档的块不必每次都请求密钥服务。
//
// 任何一步失败都由调用方退回掩码文本：看不到原文只是不方便，泄露原文才是事故（fail-closed）。
package pii

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// KeyService 解开被主密钥包裹的数据密钥。
type KeyService interface {
	Unwrap(ctx context.Context, keyID, wrapped string) ([]byte, error)
}

// Restorer 还原一个块的 PII 原值。
type Restorer struct {
	Keys KeyService
	// TTL 与 Max 控制数据密钥的内存缓存
	TTL time.Duration
	Max int
	Now func() time.Time

	mu    sync.Mutex
	cache map[string]cached
}

type cached struct {
	key []byte
	at  time.Time
}

// ErrMalformed：密文或回填位置与正文对不上。
var ErrMalformed = errors.New("PII 密文格式不对")

// Restore 返回把原值回填后的正文。text 必须是入库时写入的掩码正文（位置按 Unicode 字符计）。
func (r *Restorer) Restore(ctx context.Context, chunkID, text string, ciphertext []byte, wrapped, keyID string) (string, error) {
	if len(ciphertext) < 12+16 || wrapped == "" {
		return "", ErrMalformed
	}
	key, err := r.key(ctx, keyID, wrapped)
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	plain, err := gcm.Open(nil, ciphertext[:12], ciphertext[12:], []byte(chunkID))
	if err != nil {
		return "", fmt.Errorf("解密失败: %w", err)
	}
	var spans [][3]any
	if err := json.Unmarshal(plain, &spans); err != nil {
		return "", ErrMalformed
	}
	runes := []rune(text)
	out := make([]rune, 0, len(runes))
	pos := 0
	for _, sp := range spans {
		start, ok1 := sp[0].(float64)
		end, ok2 := sp[1].(float64)
		orig, ok3 := sp[2].(string)
		s, e := int(start), int(end)
		if !ok1 || !ok2 || !ok3 || s < pos || e < s || e > len(runes) {
			return "", ErrMalformed
		}
		out = append(out, runes[pos:s]...)
		out = append(out, []rune(orig)...)
		pos = e
	}
	out = append(out, runes[pos:]...)
	return string(out), nil
}

func (r *Restorer) key(ctx context.Context, keyID, wrapped string) ([]byte, error) {
	now := r.Now()
	r.mu.Lock()
	if c, ok := r.cache[wrapped]; ok && now.Sub(c.at) < r.TTL {
		r.mu.Unlock()
		return c.key, nil
	}
	r.mu.Unlock()
	key, err := r.Keys.Unwrap(ctx, keyID, wrapped)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cache == nil || len(r.cache) >= r.Max {
		// 缓存满了直接清空：数据密钥按文档分，命中率主要来自同一次检索里同一文档的多个块
		r.cache = map[string]cached{}
	}
	r.cache[wrapped] = cached{key, now}
	return key, nil
}
