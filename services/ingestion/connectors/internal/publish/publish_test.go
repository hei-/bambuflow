package publish

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/common/v1"
	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/docmeta"
)

func TestAccessEventsGoToTheirOwnStream(t *testing.T) {
	mr := miniredis.RunT(t)
	p := &Publisher{Redis: redis.NewClient(&redis.Options{Addr: mr.Addr()}), SourceID: "src_a", Bucket: "b"}
	ctx := context.Background()
	now := time.Now()
	if err := p.Delete(ctx, "d1", now); err != nil {
		t.Fatal(err)
	}
	d := &Document{Meta: docmeta.Meta{DocID: "d2", Classification: "confidential", ACLTokens: []string{"group:a"}}, ContentHash: "h"}
	acl, err := p.changed("evt_t", d, ingestionv1.ChangeType_CHANGE_TYPE_ACL_ONLY, now, "s3://b/x.json")
	if err != nil {
		t.Fatal(err)
	}
	// 快速通道直接用事件里的权限与密级，不读文档包
	if acl.GetRawRef() != "" || acl.GetClassification() != commonv1.Classification_CLASSIFICATION_CONFIDENTIAL ||
		len(acl.GetAclTokens()) != 1 {
		t.Fatalf("仅权限变化的事件: %v", acl)
	}
	if err := p.emit(ctx, acl); err != nil {
		t.Fatal(err)
	}
	up, _ := p.changed("evt_t", &Document{Meta: docmeta.Meta{DocID: "d3"}}, ingestionv1.ChangeType_CHANGE_TYPE_UPSERT, now, "s3://b/y.json")
	if err := p.emit(ctx, up); err != nil {
		t.Fatal(err)
	}
	access, _ := p.Redis.XRange(ctx, AccessStream, "-", "+").Result()
	content, _ := p.Redis.XRange(ctx, Stream, "-", "+").Result()
	if len(access) != 2 || len(content) != 1 {
		t.Fatalf("删除与仅权限变化走 %s，内容变更走 %s: %d %d", AccessStream, Stream, len(access), len(content))
	}
	var evt ingestionv1.DocumentChanged
	if err := proto.Unmarshal([]byte(access[0].Values["pb"].(string)), &evt); err != nil || evt.GetChangeType() != ingestionv1.ChangeType_CHANGE_TYPE_DELETE {
		t.Fatalf("%v %v", &evt, err)
	}
	if _, err := p.changed("evt_t", &Document{Meta: docmeta.Meta{DocID: "d5", Classification: "secret"}}, ingestionv1.ChangeType_CHANGE_TYPE_ACL_ONLY, now, ""); err == nil {
		t.Fatal("不合法的密级应报错")
	}
}
