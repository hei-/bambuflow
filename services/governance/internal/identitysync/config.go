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
type Config struct {
	Source string `yaml:"source"`
	LDAP   struct {
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

func (c *Config) validate() error {
	if c.Source == "" || c.LDAP.URL == "" || c.LDAP.Users.BaseDN == "" || c.LDAP.Groups.BaseDN == "" {
		return fmt.Errorf("source、ldap.url、users.base_dn、groups.base_dn 必填")
	}
	if c.Interval <= 0 || c.FullEvery <= 0 {
		return fmt.Errorf("interval、full_every 必须大于 0")
	}
	for _, a := range []string{c.Mapping.Username, c.LDAP.Groups.NameAttr, c.LDAP.Groups.MemberAttr} {
		if !safeAttr.MatchString(a) {
			return fmt.Errorf("属性名不合法: %q", a)
		}
	}
	for grade, level := range c.Mapping.ClearanceByGrade {
		if _, ok := clearanceCodes[level]; !ok {
			return fmt.Errorf("等级 %s 映射到未知密级 %q", grade, level)
		}
	}
	return nil
}
