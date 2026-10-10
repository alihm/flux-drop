package storagepool

import (
	"testing"
)

func TestReclamationSettingsDefaultOffAndValidateLocalAdapter(t *testing.T) {
	base := map[string]string{"DROP_ROLE": "secondary", "FLUX_APP_NAME": "storagea", "DROP_PRIMARY_APP_NAME": "primarya", "DROP_STORAGE_CAPACITY_BYTES": "4294967296", "DROP_STORAGE_API_KEYS_JSON": `{"v1":"` + testKey + `"}`}
	parse := func(values map[string]string) (Config, error) {
		return FromEnv(func(key string) string { return values[key] })
	}
	c, err := parse(base)
	if err != nil || c.Reclamation || c.SyncthingURL != "" {
		t.Fatal("unsafe default", c, err)
	}
	base["DROP_STORAGE_SYNCTHING_URL"], base["DROP_STORAGE_SYNCTHING_API_KEY"], base["DROP_STORAGE_SYNCTHING_FOLDER"] = "http://127.0.0.1:8384", testKey, "drop-data"
	if _, err = parse(base); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"https://127.0.0.1:8384", "http://localhost:8384", "http://192.0.2.1:8384", "http://127.0.0.1:8384/path", "http://user:password@127.0.0.1:8384", "http://127.0.0.1:8384?secret=example", "http://127.0.0.1:8384#fragment"} {
		base["DROP_STORAGE_SYNCTHING_URL"] = value
		if _, err = parse(base); err == nil {
			t.Fatal("unsafe adapter URL", value)
		}
	}
	base["DROP_STORAGE_SYNCTHING_URL"] = "http://127.0.0.1:8384"
	base["DROP_STORAGE_SYNCTHING_API_KEY"] = ""
	if _, err = parse(base); err == nil {
		t.Fatal("incomplete adapter configuration")
	}
	base["DROP_STORAGE_SYNCTHING_API_KEY"] = testKey
	base["DROP_STORAGE_RECLAMATION_ENABLED"] = "true"
	if _, err = parse(base); err == nil {
		t.Fatal("secondary enabled primary scheduler")
	}
}
