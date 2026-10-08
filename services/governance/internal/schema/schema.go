// Package schema 是 governance 拥有的元数据库表结构（平台身份库等），由 governance migrate 执行。
package schema

import "embed"

//go:embed sql/*.sql
var Files embed.FS
