package main

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type publishingConfig struct {
	enabled                      bool
	root, user, password         string
	anonymousBytes, accountBytes int64
}

func publishingFromEnv(get func(string) string) (publishingConfig, error) {
	c := publishingConfig{root: get("DROP_DATA_DIR"), user: get("DROP_STAGING_USER"), password: get("DROP_STAGING_PASSWORD"), anonymousBytes: 1 << 30, accountBytes: 10 << 30}
	if c.root == "" {
		c.root = "/data"
	}
	if c.user == "" {
		c.user = "tester"
	}
	flag := get("DROP_PUBLISHING_ENABLED")
	if flag == "" {
		flag = "true"
	}
	if flag != "" && flag != "true" && flag != "false" {
		return c, errors.New("DROP_PUBLISHING_ENABLED must be true or false")
	}
	c.enabled = flag == "true"
	for key, target := range map[string]*int64{"DROP_ANONYMOUS_BYTE_LIMIT": &c.anonymousBytes, "DROP_ACCOUNT_BYTE_LIMIT": &c.accountBytes} {
		if raw := get(key); raw != "" {
			n, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || n < 1<<20 || n > 1<<40 {
				return c, errors.New("byte budgets must be integers from 1 MiB to 1 TiB")
			}
			*target = n
		}
	}
	if !c.enabled {
		return c, nil
	}
	if !filepath.IsAbs(c.root) || filepath.Clean(c.root) != c.root || c.root == "/" {
		return c, errors.New("DROP_DATA_DIR must be a clean absolute directory, not root")
	}
	// Passphrase-only deployments are the public service, not the historical
	// password-gated staging image. Never reuse a cluster secret for browser auth.
	if c.password == "" && len(get("DROP_CLUSTER_PASSPHRASE")) >= 32 {
		return c, nil
	}
	if len(c.user) < 1 || len(c.user) > 128 || strings.ContainsAny(c.user, ":\r\n") || len(c.password) < 20 || len(c.password) > 1024 {
		return c, errors.New("staging publishing requires DROP_STAGING_USER and a 20+ character DROP_STAGING_PASSWORD")
	}
	return c, nil
}

func validateStorage(c publishingConfig) error {
	info, err := os.Lstat(c.root)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("data root must be an existing real directory")
	}
	return nil
}

func sessionCreationBudget(get func(string) string) (int64, error) {
	raw := get("DROP_SESSION_CREATIONS_PER_MINUTE")
	if raw == "" {
		return 60, nil
	}
	budget, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || budget < 1 || budget > 10000 {
		return 0, errors.New("invalid DROP_SESSION_CREATIONS_PER_MINUTE")
	}
	return budget, nil
}
