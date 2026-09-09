package config

import (
	"testing"
	"time"
)

func valid() Config {
	return Config{Token: "test-token", Endpoint: "https://www.modelscope.cn", Repo: "owner/name", RepoType: "datasets", Revision: "master", Namespace: "00", UploadMode: "proxy", MaxBytes: 10, Timeout: time.Minute, UploadHosts: []string{".aliyuncs.com"}}
}
func TestValidation(t *testing.T) {
	c := valid()
	if e := c.Validate(); e != nil {
		t.Fatal(e)
	}
	for _, change := range []func(*Config){func(c *Config) { c.Namespace = "1" }, func(c *Config) { c.Repo = "../repo" }, func(c *Config) { c.Endpoint = "http://example.com" }, func(c *Config) { c.UploadMode = "unsafe" }, func(c *Config) { c.Token = "" }, func(c *Config) { c.Revision = "release" }, func(c *Config) { c.UploadHosts = []string{"evil.com"} }} {
		c := valid()
		change(&c)
		if e := c.Validate(); e == nil {
			t.Fatal("invalid config accepted", c)
		}
	}
	c = valid()
	c.RepoType = "models"
	c.Revision = "release"
	if e := c.Validate(); e != nil {
		t.Fatal(e)
	}
}
func TestStorageIdentity(t *testing.T) {
	a := valid()
	b := a
	b.Repo = "other/name"
	if a.Prefix() == b.Prefix() {
		t.Fatal("identity not scoped")
	}
	b = a
	b.Token = "rotated"
	if a.Prefix() != b.Prefix() {
		t.Fatal("token changed storage identity")
	}
}
