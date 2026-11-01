package cluster

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestStatusJSONFieldGolden(t *testing.T) {
	want := strings.Fields("appliedIndex backupFailures bootstrapView clusterID durableRevision historyDigest id lastContactAgeSeconds lastIndex lastSuccessfulBackup leaderID learners quorumVerified replicatedRevision role stateIndex term uptimeSeconds voters")
	var got []string
	typ := reflect.TypeOf(Status{})
	for i := 0; i < typ.NumField(); i++ {
		got = append(got, strings.Split(typ.Field(i).Tag.Get("json"), ",")[0])
	}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Status is a pinned rolling-upgrade contract: got %v want %v", got, want)
	}
}
func TestPeerStatusAcceptsExtensionsButValidatesKnownFields(t *testing.T) {
	issue := materials(t)
	status := map[string]any{"clusterID": testClusterID, "id": "peer", "role": "Follower", "leaderID": "leader", "uptimeSeconds": 2, "lastIndex": 4, "appliedIndex": 3, "stateIndex": 2, "quorumVerified": false, "futureField": map[string]any{"enabled": true}}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { json.NewEncoder(w).Encode(status) }))
	server.TLS = issue("peer").Server.Clone()
	server.StartTLS()
	defer server.Close()
	observer, err := NewObserver(&staticPeers{fresh: true}, testClusterID, "testapp", issue("caller").Client)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.client.CloseIdleConnections()
	address := netip.MustParseAddrPort(server.Listener.Addr().String())
	if _, err := observer.probe(context.Background(), address); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		key   string
		value any
	}{{"stateIndex", 5}, {"role", "Bogus"}, {"quorumVerified", true}, {"uptimeSeconds", -1}, {"id", "impostor"}, {"clusterID", "other"}, {"lastIndex", "four"}} {
		old := status[test.key]
		status[test.key] = test.value
		if _, err := observer.probe(context.Background(), address); err == nil {
			t.Fatal("invalid known field accepted", test.key)
		}
		status[test.key] = old
	}
	if decodeStrict([]byte(`{"futureField":true}`), &Status{}) == nil {
		t.Fatal("state/command strict decoding was relaxed")
	}
}
