package logging

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

const (
	jwtSample = "eyJhbGciOiJFUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.c2lnbmF0dXJl"
	phone     = "13812345678"
	idCard    = "11010119900307777X"
	bankCard  = "6222021234567890123"
	email     = "alice@corp.example"
)

func TestSensitiveValuesNeverReachOutput(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, "test", slog.LevelDebug)

	log.Info("用户 "+phone+" 提交了身份证 "+idCard+"，联系 "+email,
		slog.String("authorization", "Bearer "+jwtSample),
		slog.String("Client-Secret", "s3cr3t-value"),
		slog.String("note", "收款卡号 "+bankCard),
		slog.Any("err", errors.New("上游拒绝令牌 "+jwtSample)),
		slog.Group("req", slog.String("password", "p@ss"), slog.String("header", "bearer abc.def-123")),
		slog.String("question", "请查询客户的贷款余额"),
	)

	out := buf.String()
	for _, secret := range []string{jwtSample, phone, idCard, bankCard, email, "s3cr3t-value", "p@ss", "abc.def-123", "贷款余额"} {
		if strings.Contains(out, secret) {
			t.Errorf("日志中出现了敏感内容 %q:\n%s", secret, out)
		}
	}
	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("输出应是一行 JSON: %v", err)
	}
	if rec["service"] != "test" {
		t.Errorf("缺少 service 字段: %v", rec)
	}
}

func TestOrdinaryNumbersKept(t *testing.T) {
	// 不应误伤：短数字、版本号、耗时
	in := "索引版本 v20261004-3，耗时 1234 ms，返回 20 条"
	if got := Redact(in); got != in {
		t.Errorf("Redact(%q) = %q", in, got)
	}
}

func TestIDCardNotSplitAsPhone(t *testing.T) {
	got := Redact("证件 " + idCard)
	if !strings.Contains(got, "[已脱敏:id_card]") || strings.Contains(got, "phone") {
		t.Errorf("身份证号应整体识别: %q", got)
	}
}
