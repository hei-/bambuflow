package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/hei-/bambuflow/libs/platform-go/denyrule"
	"github.com/hei-/bambuflow/libs/platform-go/vault"
)

// loadDenyKey 从 Vault 读备用通道的签名私钥；读不到就不启动（规则发不到备用通道）。
func loadDenyKey(ctx context.Context, vc *vault.Client) (*ecdsa.PrivateKey, error) {
	data, err := vc.ReadKV(ctx, envOr("BF_VAULT_KV_MOUNT", "secret"), denyrule.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("读取紧急拒绝规则的签名密钥: %w", err)
	}
	return denyrule.ParsePrivateKey(data["private_key"])
}

// runEmergencyDeny 是运维的签名工具（governance 详设 8.2）：governance 服务整体不可用时，信息安全用自己的 Vault 凭证
// （VAULT_ADDR、VAULT_TOKEN）取签名私钥签一条拒绝规则，追加到事件流的独立主题（-publish，需要 BF_REDIS_ADDR），
// 或写成文件（-out）由部署系统投放到 retrieval 的配置卷。两种方式都只能增加拒绝。
// 事后仍要经管理接口登记同一条规则，让它写进索引库的拒绝名单并留下变更审计。
func runEmergencyDeny(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("emergency-deny", flag.ContinueOnError)
	scope := fs.String("scope", "", "source、document 或 tenant")
	target := fs.String("target", "", "数据源 ID、文档 ID 或租户名")
	reason := fs.String("reason", "", "原因（如内部工单号）")
	by := fs.String("by", "", "签发人（平台账号）")
	out := fs.String("out", "", "写成规则文件的路径")
	publish := fs.Bool("publish", false, "追加到事件流的独立主题")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if (*out == "") == !*publish {
		return errors.New("-out 与 -publish 恰好选一个")
	}
	id := make([]byte, 12)
	if _, err := rand.Read(id); err != nil {
		return err
	}
	rule := denyrule.Rule{ID: "den_" + hex.EncodeToString(id), Scope: *scope, Target: *target, Reason: *reason,
		IssuedBy: *by, IssuedAt: time.Now().UTC()}
	if err := rule.Validate(); err != nil {
		return fmt.Errorf("%w（-scope、-target、-reason、-by 都必填）", err)
	}
	vc, err := vault.FromEnv()
	if err != nil {
		return err
	}
	key, err := loadDenyKey(ctx, vc)
	if err != nil {
		return err
	}
	signed, err := denyrule.Sign(key, rule)
	if err != nil {
		return err
	}
	if *out != "" {
		if err := os.WriteFile(filepath.Clean(*out), signed, 0o600); err != nil {
			return err
		}
	} else {
		addr, err := env("BF_REDIS_ADDR")
		if err != nil {
			return err
		}
		rdb := redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("BF_REDIS_PASSWORD")})
		defer func() { _ = rdb.Close() }()
		if err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: denyrule.Stream, Values: map[string]any{"rule": signed}}).Err(); err != nil {
			return err
		}
	}
	_, err = fmt.Fprintf(stdout, "已签发 %s（%s %s）\n", rule.ID, rule.Scope, rule.Target)
	return err
}
