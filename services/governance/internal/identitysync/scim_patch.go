package identitysync

import (
	"regexp"
	"strings"
)

// scimPath 是 PATCH 的路径（RFC 7644 3.5.2）：[schema URN:]attr[filter][.sub]，filter 只支持 “子属性 eq 值”。
type scimPath struct {
	container               string // 扩展 schema 的 URN；为空表示核心属性
	attr                    string
	filterAttr, filterValue string
	hasFilter               bool
	sub                     string
}

var pathRe = regexp.MustCompile(`^(\w+)(?:\[\s*(\w+)\s+eq\s+(?:"((?:[^"\\]|\\.)*)"|(true|false))\s*\])?(?:\.(\w+))?$`)

func parsePath(path string) (scimPath, error) {
	var p scimPath
	rest := path
	if strings.HasPrefix(strings.ToLower(path), "urn:") {
		i := strings.LastIndex(path, ":")
		p.container, rest = path[:i], path[i+1:]
	}
	m := pathRe.FindStringSubmatch(rest)
	if m == nil {
		return p, badRequest("invalidPath", "不支持的路径 %q", path)
	}
	p.attr, p.sub = m[1], m[5]
	if m[2] != "" {
		p.hasFilter, p.filterAttr, p.filterValue = true, m[2], m[3]+m[4]
	}
	return p, nil
}

// applyOp 执行一个 PATCH 操作。不带路径时 value 是对象，逐项按同一个 op 处理（扩展 schema 的对象按其中的属性展开）。
func applyOp(body map[string]any, op, path string, v any) error {
	if op != "add" && op != "replace" && op != "remove" {
		return badRequest("invalidSyntax", "不支持的 op %q", op)
	}
	if path == "" {
		obj, ok := v.(map[string]any)
		if op == "remove" || !ok {
			return badRequest("noTarget", "不带 path 时只能 add 或 replace 一个对象")
		}
		for k, val := range obj {
			if inner, ok := val.(map[string]any); ok && strings.HasPrefix(strings.ToLower(k), "urn:") {
				for sub, sv := range inner {
					if err := applyOp(body, op, k+":"+sub, sv); err != nil {
						return err
					}
				}
				continue
			}
			if err := applyOp(body, op, k, val); err != nil {
				return err
			}
		}
		return nil
	}
	p, err := parsePath(path)
	if err != nil {
		return err
	}
	target := body
	if p.container != "" {
		k := key(body, p.container)
		inner, ok := body[k].(map[string]any)
		if !ok {
			if op == "remove" {
				return nil
			}
			inner = map[string]any{}
			body[k] = inner
		}
		target = inner
	}
	k := key(target, p.attr)
	switch {
	case !p.hasFilter && p.sub == "":
		return setAttr(target, k, op, v)
	case !p.hasFilter:
		obj, ok := target[k].(map[string]any)
		if !ok {
			if _, isList := target[k].([]any); isList {
				return badRequest("invalidPath", "多值属性 %s 要用过滤条件选出某一项", p.attr)
			}
			if op == "remove" {
				return nil
			}
			obj = map[string]any{}
			target[k] = obj
		}
		if op == "remove" {
			delete(obj, key(obj, p.sub))
		} else {
			obj[key(obj, p.sub)] = v
		}
		return nil
	}
	list, _ := target[k].([]any)
	var kept []any
	matched := false
	for _, it := range list {
		obj, ok := it.(map[string]any)
		if !ok || !matches(obj, p.filterAttr, p.filterValue) {
			kept = append(kept, it)
			continue
		}
		matched = true
		switch {
		case op == "remove" && p.sub == "":
			continue
		case op == "remove":
			delete(obj, key(obj, p.sub))
		case p.sub != "":
			obj[key(obj, p.sub)] = v
		default:
			val, ok := v.(map[string]any)
			if !ok {
				return badRequest("invalidValue", "%s 的一项必须是对象", p.attr)
			}
			for vk, vv := range val {
				obj[key(obj, vk)] = vv
			}
		}
		kept = append(kept, obj)
	}
	if !matched && op != "remove" {
		// 没有符合条件的一项：按条件新建一项（如 emails[type eq "work"].value 第一次出现）
		item := map[string]any{p.filterAttr: filterLiteral(p.filterValue)}
		if p.sub != "" {
			item[p.sub] = v
		} else if val, ok := v.(map[string]any); ok {
			for vk, vv := range val {
				item[vk] = vv
			}
		}
		kept = append(kept, item)
	}
	target[k] = kept
	return nil
}

// setAttr 处理不带过滤条件与子属性的路径：多值属性的 add 是追加（按 value 去重），remove 带 value 时只移除这些项。
func setAttr(target map[string]any, k, op string, v any) error {
	cur, isList := target[k].([]any)
	switch op {
	case "replace":
		target[k] = v
	case "add":
		if !isList {
			target[k] = v
			return nil
		}
		add, ok := v.([]any)
		if !ok {
			add = []any{v}
		}
		for _, it := range add {
			if !containsValue(cur, it) {
				cur = append(cur, it)
			}
		}
		target[k] = cur
	case "remove":
		drop, ok := v.([]any)
		if !isList || !ok {
			delete(target, k)
			return nil
		}
		var kept []any
		for _, it := range cur {
			if !containsValue(drop, it) {
				kept = append(kept, it)
			}
		}
		target[k] = kept
	}
	return nil
}

// containsValue：多值属性的项按 value 子属性比较（如群组成员），没有 value 的按整体比较。
func containsValue(list []any, it any) bool {
	val := valueOf(it)
	for _, x := range list {
		if val != nil && valueOf(x) == val {
			return true
		}
	}
	return false
}

func valueOf(it any) any {
	if obj, ok := it.(map[string]any); ok {
		return obj["value"]
	}
	return it
}

func matches(obj map[string]any, attr, want string) bool {
	switch got := obj[key(obj, attr)].(type) {
	case string:
		return strings.EqualFold(got, want)
	case bool:
		return (want == "true") == got
	}
	return false
}

func filterLiteral(v string) any {
	switch v {
	case "true":
		return true
	case "false":
		return false
	}
	return v
}

// key 返回 m 里与 name 不区分大小写相同的键（SCIM 的属性名不区分大小写），没有就用 name。
func key(m map[string]any, name string) string {
	if _, ok := m[name]; ok {
		return name
	}
	for k := range m {
		if strings.EqualFold(k, name) {
			return k
		}
	}
	return name
}
