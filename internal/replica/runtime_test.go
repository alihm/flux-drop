package replica

import "testing"

func TestRuntimeEnvironment(t *testing.T) {
	env := map[string]string{"DROP_PEERS_ENABLED": "true", "FLUX_APP_NAME": "drop", "DROP_INSTANCE_ID": "one", "REPLICA_PORT": "8444", "DROP_REPLICA_SELF_IPS": "91.192.45.220,74.103.5.187", "DROP_DATA_DIR": "/data", "DROP_PEER_CERT_FILE": "/run/secrets/peer.crt", "DROP_PEER_KEY_FILE": "/run/secrets/peer.key", "DROP_PEER_CA_FILE": "/run/secrets/ca.crt"}
	get := func(k string) string { return env[k] }
	c, err := RuntimeConfigFromEnv(get)
	if err != nil || c.Port != 8444 || len(c.Self) != 2 {
		t.Fatal(c, err)
	}
	for _, tc := range []struct{ key, value string }{
		{"DROP_PEERS_ENABLED", "yes"}, {"FLUX_APP_NAME", "../app"}, {"DROP_INSTANCE_ID", ""}, {"REPLICA_PORT", "0"}, {"REPLICA_PORT", "8081"}, {"REPLICA_PORT", "65536"}, {"DROP_REPLICA_SELF_IPS", ""}, {"DROP_REPLICA_SELF_IPS", "127.0.0.1"}, {"DROP_REPLICA_SELF_IPS", "example.com"}, {"DROP_DATA_DIR", "/"}, {"DROP_PEER_KEY_FILE", "relative.key"},
	} {
		old := env[tc.key]
		env[tc.key] = tc.value
		if _, err := RuntimeConfigFromEnv(get); err == nil {
			t.Errorf("accepted %s=%s", tc.key, tc.value)
		}
		env[tc.key] = old
	}
	env["DROP_PEERS_ENABLED"] = "false"
	env["DROP_PEER_LISTEN_PORT"] = "8445"
	env["DROP_REPLICA_SELF_IPS"] = ""
	env["FLUX_NODE_HOST_IP"] = "91.192.45.220"
	env["DROP_PEERS_ENABLED"] = "true"
	c, err = RuntimeConfigFromEnv(get)
	if err != nil || c.ListenPort != 8445 || c.Port != 8444 || len(c.Self) != 1 {
		t.Fatal(c, err)
	}
	env["DROP_PEER_LISTEN_PORT"] = "8080"
	if _, err = RuntimeConfigFromEnv(get); err == nil {
		t.Fatal("reserved listen port")
	}
	env["DROP_PEERS_ENABLED"] = "false"
	if c, err := RuntimeConfigFromEnv(get); c != nil || err != nil {
		t.Fatal(c, err)
	}
}
