// Package storagepool implements private, app-scoped storage for Flux Drop.
package storagepool

import (
	"encoding/json"
	"errors"
	"io"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

const DefaultPort = 34447
const Headroom = int64(1 << 30)

// A stored ZIP can add two 1,024-byte names and headers for each of 5,000 files.
const MaxBody = int64(216 << 20)

var appRE = regexp.MustCompile(`^[A-Za-z0-9]{1,64}$`)
var syncFolderRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)
var keyRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)
var idRE = regexp.MustCompile(`^[a-f0-9]{32}$`)
var digestRE = regexp.MustCompile(`^[a-f0-9]{64}$`)

// Secrets are intentionally excluded from operational JSON responses.
type App struct {
	AppName string `json:"appName"`
	Port    uint16 `json:"port"`
	KeyID   string `json:"keyId"`
	APIKey  string `json:"apiKey"`
	Drain   bool   `json:"drain,omitempty"`
}
type Config struct {
	Role, AppName, PrimaryApp                      string
	Apps                                           []App
	Keys                                           map[string]string
	Port                                           uint16
	Capacity, CacheBytes                           int64
	AdminKey                                       string
	Reclamation                                    bool
	SyncthingURL, SyncthingAPIKey, SyncthingFolder string
}

func decode(raw string, value any) error {
	if len(raw) > 64<<10 {
		return errors.New("configuration too large")
	}
	d := json.NewDecoder(strings.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return errors.New("invalid storage configuration JSON")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return errors.New("trailing storage configuration JSON")
	}
	return nil
}
func FromEnv(get func(string) string) (Config, error) {
	c := Config{Role: get("DROP_ROLE"), AppName: get("FLUX_APP_NAME"), Port: DefaultPort, CacheBytes: 256 << 20, AdminKey: get("DROP_STORAGE_ADMIN_KEY")}
	if raw := get("DROP_STORAGE_RECLAMATION_ENABLED"); raw != "" {
		v, err := strconv.ParseBool(raw)
		if err != nil {
			return c, errors.New("invalid DROP_STORAGE_RECLAMATION_ENABLED")
		}
		c.Reclamation = v
	}
	c.SyncthingURL, c.SyncthingAPIKey, c.SyncthingFolder = get("DROP_STORAGE_SYNCTHING_URL"), get("DROP_STORAGE_SYNCTHING_API_KEY"), get("DROP_STORAGE_SYNCTHING_FOLDER")
	if c.SyncthingURL != "" || c.SyncthingAPIKey != "" || c.SyncthingFolder != "" {
		u, err := url.Parse(c.SyncthingURL)
		if err != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" && u.Path != "/" || !validSecret(c.SyncthingAPIKey) || !syncFolderRE.MatchString(c.SyncthingFolder) {
			return c, errors.New("Syncthing cleanup requires a loopback HTTP URL, API key and folder ID")
		}
		ip, err := netip.ParseAddr(u.Hostname())
		if err != nil || !ip.IsLoopback() {
			return c, errors.New("Syncthing API must use a loopback IP")
		}
	}
	if c.Role == "" {
		for _, k := range []string{"DROP_STORAGE_APPS_JSON", "DROP_PRIMARY_APP_NAME", "DROP_STORAGE_API_KEYS_JSON", "DROP_STORAGE_CAPACITY_BYTES", "DROP_STORAGE_ADMIN_KEY", "DROP_CACHE_BYTES", "DROP_STORAGE_PORT", "DROP_STORAGE_RECLAMATION_ENABLED", "DROP_STORAGE_SYNCTHING_URL", "DROP_STORAGE_SYNCTHING_API_KEY", "DROP_STORAGE_SYNCTHING_FOLDER"} {
			if get(k) != "" {
				return c, errors.New("storage settings require DROP_ROLE")
			}
		}
		return c, nil
	}
	if c.Role != "primary" && c.Role != "secondary" {
		return c, errors.New("DROP_ROLE must be primary or secondary")
	}
	if !appRE.MatchString(c.AppName) {
		return c, errors.New("storage roles require a valid Flux app name")
	}
	for k, target := range map[string]*int64{"DROP_STORAGE_CAPACITY_BYTES": &c.Capacity, "DROP_CACHE_BYTES": &c.CacheBytes} {
		if v := get(k); v != "" {
			n, e := strconv.ParseInt(v, 10, 64)
			if e != nil || n < 0 || n > 1<<40 {
				return c, errors.New("invalid storage byte limit")
			}
			*target = n
		}
	}
	if v := get("DROP_STORAGE_PORT"); v != "" {
		n, e := strconv.ParseUint(v, 10, 16)
		if e != nil || n < 1024 || n == 8080 || n == 8081 || n == 34444 || n == 34445 || n == 34446 {
			return c, errors.New("invalid DROP_STORAGE_PORT")
		}
		c.Port = uint16(n)
	}
	if c.Role == "primary" {
		if c.SyncthingURL != "" {
			return c, errors.New("Syncthing settings require secondary role")
		}
		if get("DROP_PRIMARY_APP_NAME") != "" || get("DROP_STORAGE_API_KEYS_JSON") != "" || c.Capacity != 0 {
			return c, errors.New("secondary settings supplied to primary")
		}
		if err := decode(get("DROP_STORAGE_APPS_JSON"), &c.Apps); err != nil {
			return c, err
		}
		if len(c.Apps) < 1 || len(c.Apps) > 64 {
			return c, errors.New("configure between 1 and 64 storage apps")
		}
		seen := map[string]bool{}
		for i := range c.Apps {
			a := &c.Apps[i]
			if a.Port == 0 {
				a.Port = DefaultPort
			}
			if !appRE.MatchString(a.AppName) || strings.EqualFold(a.AppName, c.AppName) || seen[strings.ToLower(a.AppName)] || !keyRE.MatchString(a.KeyID) || !validSecret(a.APIKey) || a.Port < 1024 || a.Port == 8080 || a.Port == 8081 {
				return c, errors.New("invalid or duplicate storage app")
			}
			seen[strings.ToLower(a.AppName)] = true
		}
		if c.AdminKey != "" && !validSecret(c.AdminKey) {
			return c, errors.New("invalid DROP_STORAGE_ADMIN_KEY")
		}
	} else {
		if get("DROP_STORAGE_APPS_JSON") != "" || c.AdminKey != "" || get("DROP_CACHE_BYTES") != "" {
			return c, errors.New("primary settings supplied to secondary")
		}
		if c.Reclamation {
			return c, errors.New("reclamation scheduling requires primary role")
		}
		c.PrimaryApp = get("DROP_PRIMARY_APP_NAME")
		if !appRE.MatchString(c.PrimaryApp) || strings.EqualFold(c.PrimaryApp, c.AppName) || c.Capacity <= Headroom {
			return c, errors.New("secondary requires primary app name and capacity exceeding 1 GiB")
		}
		if err := decode(get("DROP_STORAGE_API_KEYS_JSON"), &c.Keys); err != nil {
			return c, err
		}
		if len(c.Keys) < 1 || len(c.Keys) > 4 {
			return c, errors.New("configure between 1 and 4 storage API keys")
		}
		for id, secret := range c.Keys {
			if !keyRE.MatchString(id) || !validSecret(secret) {
				return c, errors.New("invalid storage API key")
			}
		}
	}
	return c, nil
}
func validSecret(s string) bool {
	return len(s) >= 32 && len(s) <= 1024 && !strings.ContainsAny(s, "\r\n\x00")
}
