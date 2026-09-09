package config

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	Addr, RedisURL, Endpoint, Token, Repo, RepoType, Revision, Namespace, UploadMode string
	MaxBytes                                                                         int64
	Timeout                                                                          time.Duration
	UploadHosts                                                                      []string
}

func Load() (Config, error) {
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		return Config{}, fmt.Errorf("read .env: %w", err)
	}
	c := Config{Addr: env("LISTEN_ADDR", "127.0.0.1:8080"), RedisURL: env("REDIS_URL", "redis://localhost:6379/0"), Endpoint: env("MODELSCOPE_ENDPOINT", "https://www.modelscope.cn"), Token: os.Getenv("MODELSCOPE_TOKEN"), Repo: os.Getenv("MODELSCOPE_REPO_ID"), RepoType: env("MODELSCOPE_REPO_TYPE", "datasets"), Revision: env("MODELSCOPE_REVISION", "master"), Namespace: env("FILECABINET_NAMESPACE", "00"), UploadMode: env("UPLOAD_MODE", "proxy"), UploadHosts: strings.Split(env("STORAGE_HOST_SUFFIXES", ".modelscope.cn,.aliyuncs.com,.amazonaws.com"), ",")}
	var err error
	c.MaxBytes, err = strconv.ParseInt(env("MAX_UPLOAD_BYTES", "10737418240"), 10, 64)
	if err != nil || c.MaxBytes < 1 || c.MaxBytes > 1<<50 {
		return c, fmt.Errorf("invalid MAX_UPLOAD_BYTES")
	}
	c.Timeout, err = time.ParseDuration(env("TRANSFER_TIMEOUT", "30m"))
	if err != nil || c.Timeout < time.Second || c.Timeout > 24*time.Hour {
		return c, fmt.Errorf("invalid TRANSFER_TIMEOUT")
	}
	return c, c.Validate()
}
func (c Config) Validate() error {
	if c.Token == "" || strings.ContainsAny(c.Token, "\r\n;") {
		return fmt.Errorf("MODELSCOPE_TOKEN is required and must be a single token")
	}
	if !regexp.MustCompile(`^[A-Za-z0-9_-]+/[A-Za-z0-9_.-]+$`).MatchString(c.Repo) {
		return fmt.Errorf("MODELSCOPE_REPO_ID must be owner/name")
	}
	if c.RepoType != "models" && c.RepoType != "datasets" {
		return fmt.Errorf("MODELSCOPE_REPO_TYPE must be models or datasets")
	}
	if c.RepoType == "datasets" && c.Revision != "master" {
		return fmt.Errorf("datasets currently require MODELSCOPE_REVISION=master: upstream deletion does not accept Revision")
	}
	if !regexp.MustCompile(`^\d{2}$`).MatchString(c.Namespace) {
		return fmt.Errorf("FILECABINET_NAMESPACE must be two digits")
	}
	if !regexp.MustCompile(`^[A-Za-z0-9_.-]+$`).MatchString(c.Revision) || c.Revision == "." || c.Revision == ".." {
		return fmt.Errorf("invalid MODELSCOPE_REVISION")
	}
	if c.UploadMode != "proxy" && c.UploadMode != "direct" {
		return fmt.Errorf("UPLOAD_MODE must be proxy or direct")
	}
	u, e := url.Parse(c.Endpoint)
	if e != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return fmt.Errorf("MODELSCOPE_ENDPOINT must be an HTTPS origin")
	}
	for _, s := range c.UploadHosts {
		if !regexp.MustCompile(`^\.[a-z0-9.-]+$`).MatchString(s) {
			return fmt.Errorf("invalid STORAGE_HOST_SUFFIXES")
		}
	}
	return nil
}
func (c Config) Prefix() string {
	h := sha256.Sum256([]byte(strings.TrimRight(c.Endpoint, "/") + "/" + c.RepoType + "/" + c.Repo + "/" + c.Revision + "/" + c.Namespace))
	return "fc:" + hex.EncodeToString(h[:12]) + ":"
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
