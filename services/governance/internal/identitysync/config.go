package identitysync

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"go.yaml.in/yaml/v3"
)

// Config 是身份同步的部署配置（部署配置清单 N2：上游目录与字段映射）。口令不写在文件里，从环境变量读。
// 上游三选一（gateway 详设 3.5）：ldap、scim、file 只能配置一个。
type Config struct {
	Source string `yaml:"source"`
	// SCIM：上游（目录或 IAM）推送到平台的 SCIM 2.0 端点（scim.go）
	SCIM struct {
		// Listen 是 SCIM 端点的监听地址，如 :8090
		Listen string `yaml:"listen"`
		// TokenEnv 是存放上游调用凭证（Bearer 令牌）的环境变量名
		TokenEnv string `yaml:"token_env"`
	} `yaml:"scim"`
	// File：上游定时放置的导出文件（CSV，首行为列名），每次整份替换（file.go）
	File struct {
		Path string `yaml:"path"`
		// MaxAge：文件超过这么久没有更新就算同步失败（撤权时效取决于导出频率，须对该部署单独声明）
		MaxAge time.Duration `yaml:"max_age"`
		// GroupsColumn 是用户所属群组的列，多个群组用 GroupsSeparator 分隔（默认 ;）
		GroupsColumn    string `yaml:"groups_column"`
		GroupsSeparator string `yaml:"groups_separator"`
	} `yaml:"file"`
	LDAP struct {
		URL             string `yaml:"url"`
		BindDN          string `yaml:"bind_dn"`
		BindPasswordEnv string `yaml:"bind_password_env"`
		Users           struct {
			BaseDN string `yaml:"base_dn"`
			Filter string `yaml:"filter"`
		} `yaml:"users"`
		Groups struct {
			BaseDN     string `yaml:"base_dn"`
			Filter     string `yaml:"filter"`
			NameAttr   string `yaml:"name_attr"`
			MemberAttr string `yaml:"member_attr"`
		} `yaml:"groups"`
	} `yaml:"ldap"`
	Mapping struct {
		Username    string `yaml:"username"`
		DisplayName string `yaml:"display_name"`
		Dept        string `yaml:"dept"`
		Title       string `yaml:"title"`
		Region      string `yaml:"region"`
		Grade       string `yaml:"grade"`
		// 公司等级 → 平台密级（public / internal / confidential / restricted）；表里没有的等级按最低（public）处理
		ClearanceByGrade map[string]string `yaml:"clearance_by_grade"`
		// 可选：表示账号停用的属性及其取值；不配置时只有“从目录中消失”才视为停用（全量对账发现）
		Status struct {
			Attr           string   `yaml:"attr"`
			DisabledValues []string `yaml:"disabled_values"`
		} `yaml:"status"`
	} `yaml:"mapping"`
	Interval  time.Duration `yaml:"interval"`
	FullEvery time.Duration `yaml:"full_every"`
}

var clearanceCodes = map[string]int{"public": 1, "internal": 2, "confidential": 3, "restricted": 4}

var safeAttr = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9-]*$`)

func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &c, c.validate()
}

// 上游的种类
const (
	KindLDAP = "ldap"
	KindSCIM = "scim"
	KindFile = "file"
)

// Kind 返回配置的上游种类；validate 保证恰好一种。
func (c *Config) Kind() string {
	switch {
	case c.SCIM.Listen != "":
		return KindSCIM
	case c.File.Path != "":
		return KindFile
	}
	return KindLDAP
}

func (c *Config) validate() error {
	n := 0
	for _, set := range []bool{c.LDAP.URL != "", c.SCIM.Listen != "", c.File.Path != ""} {
		if set {
			n++
		}
	}
	if c.Source == "" || n != 1 {
		return fmt.Errorf("source 必填，ldap.url、scim.listen、file.path 恰好配置一个")
	}
	if c.Interval <= 0 {
		return fmt.Errorf("interval 必须大于 0")
	}
	if c.Mapping.Username == "" {
		return fmt.Errorf("mapping.username 必填")
	}
	switch c.Kind() {
	case KindLDAP:
		if c.LDAP.Users.BaseDN == "" || c.LDAP.Groups.BaseDN == "" || c.FullEvery <= 0 {
			return fmt.Errorf("ldap 的 users.base_dn、groups.base_dn 必填，full_every 必须大于 0")
		}
		for _, a := range []string{c.Mapping.Username, c.LDAP.Groups.NameAttr, c.LDAP.Groups.MemberAttr} {
			if !safeAttr.MatchString(a) {
				return fmt.Errorf("属性名不合法: %q", a)
			}
		}
	case KindSCIM:
		if c.SCIM.TokenEnv == "" {
			return fmt.Errorf("scim.token_env 必填")
		}
	case KindFile:
		if c.File.MaxAge <= 0 || c.File.GroupsColumn == "" {
			return fmt.Errorf("file.max_age 必须大于 0，file.groups_column 必填")
		}
		if c.File.GroupsSeparator == "" {
			c.File.GroupsSeparator = ";"
		}
	}
	for grade, level := range c.Mapping.ClearanceByGrade {
		if _, ok := clearanceCodes[level]; !ok {
			return fmt.Errorf("等级 %s 映射到未知密级 %q", grade, level)
		}
	}
	return nil
}
