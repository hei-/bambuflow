// Package schema 是 qa/batch 拥有的表结构（batch schema），由 batch migrate 执行。
package schema

import "embed"

//go:embed sql/*.sql
var Files embed.FS
