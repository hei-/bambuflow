package snapshot

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
)

// Cache 是快照的本地磁盘缓存（governance 详设 8.1）：保留当前与上一份快照，内容、签名与验签公钥一起写盘。
// 实例重启时 governance 不可用就先用盘上的快照启动，再向 governance 对齐；磁盘为空的新实例拿不到快照仍不接流量。
//
// 盘上的公钥只用来发现文件损坏或截断（缓存目录是实例自己的卷，能改它的人也能改进程）；Kubernetes 里用 emptyDir，
// 容器重启时还在，Pod 重新调度后为空（即新实例）。文件格式与 Python 的 bambuflow_platform.snapshot.SnapshotCache 相同。
type Cache struct {
	Dir     string
	Service string
	// Keys 提供已验签快照的公钥（*Verifier）
	Keys interface{ PublicKey(id string) []byte }
	Now  func() time.Time
}

// ErrNoCache：盘上没有可用的快照。
var ErrNoCache = errors.New("没有本地快照")

type entry struct {
	SignedPayload []byte    `json:"signed_payload"`
	Signature     []byte    `json:"signature"`
	SigningKeyID  string    `json:"signing_key_id"`
	PublicKey     []byte    `json:"public_key"`
	ConfirmedAt   time.Time `json:"confirmed_at"`
}

func (c *Cache) path(slot string) string {
	return filepath.Join(c.Dir, c.Service+"."+slot+".json")
}

// Save 记下一份已验签、已加载的快照。与当前一份不同时，当前一份转为上一份；相同时只更新确认时间。
func (c *Cache) Save(signed *configv1.GetSnapshotResponse) error {
	der := c.Keys.PublicKey(signed.GetSigningKeyId())
	if der == nil {
		return ErrInvalid
	}
	e := entry{SignedPayload: signed.GetSignedPayload(), Signature: signed.GetSignature(),
		SigningKeyID: signed.GetSigningKeyId(), PublicKey: der, ConfirmedAt: c.Now().UTC()}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if cur, err := read(c.path("current")); err == nil && !bytes.Equal(cur.SignedPayload, e.SignedPayload) {
		if err := os.Rename(c.path("current"), c.path("previous")); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(c.Dir, 0o700); err != nil {
		return err
	}
	// 先写临时文件再改名，写到一半崩溃不会留下半个文件
	tmp, err := os.CreateTemp(c.Dir, c.Service+".*.tmp")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), c.path("current"))
}

// Load 返回盘上的快照本体与它最后一次确认生效的时间：先取当前一份，验不过（损坏）再取上一份。
func (c *Cache) Load() (*configv1.GetSnapshotResponse, time.Time, error) {
	for _, slot := range []string{"current", "previous"} {
		e, err := read(c.path(slot))
		if err != nil {
			continue
		}
		pub, ok := parseKey(e.PublicKey)
		if !ok {
			continue
		}
		snap, err := verify(pub, &configv1.GetSnapshotResponse{SignedPayload: e.SignedPayload, Signature: e.Signature})
		if err != nil || snap.GetSnapshotVersion() == "" {
			continue
		}
		return snap, e.ConfirmedAt, nil
	}
	return nil, time.Time{}, ErrNoCache
}

func read(path string) (entry, error) {
	var e entry
	b, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return e, err
	}
	err = json.Unmarshal(b, &e)
	return e, err
}
