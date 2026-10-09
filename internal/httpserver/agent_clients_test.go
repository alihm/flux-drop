package httpserver

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"

	"github.com/runonflux/flux-drop/internal/project"
)

// VS Code's document, verbatim, including extension metadata and device flow.
const vscodeClientMetadata = `{"client_name":"Visual Studio Code","logo_uri":"https://code.visualstudio.com/assets/branding/code-stable.png","grant_types":["authorization_code","refresh_token","urn:ietf:params:oauth:grant-type:device_code"],"response_types":["code"],"token_endpoint_auth_method":"none","application_type":"native","client_id":"https://vscode.dev/oauth/client-metadata.json","client_uri":"https://vscode.dev/product","redirect_uris":["http://127.0.0.1:33418/","https://vscode.dev/redirect"]}`

func TestAgentOAuthClientTypeNormalization(t *testing.T) {
	const id = "https://vscode.dev/oauth/client-metadata.json"
	const grants = `"grant_types":["authorization_code","refresh_token","urn:ietf:params:oauth:grant-type:device_code"]`
	const responses = `"response_types":["code"]`
	both := []string{"authorization_code", "refresh_token"}
	for _, tc := range []struct {
		name, document string
		grants         []string // nil means incompatible with authorization code.
	}{
		{"vscode verbatim", vscodeClientMetadata, both},
		{"extra responses", strings.Replace(vscodeClientMetadata, responses, `"response_types":["token","code","id_token"]`, 1), both},
		{"authorization without refresh", strings.Replace(vscodeClientMetadata, grants, `"grant_types":["urn:ietf:params:oauth:grant-type:device_code","authorization_code"]`, 1), []string{"authorization_code"}},
		{"device only", strings.Replace(vscodeClientMetadata, grants, `"grant_types":["urn:ietf:params:oauth:grant-type:device_code"]`, 1), nil},
		{"refresh only", strings.Replace(vscodeClientMetadata, grants, `"grant_types":["refresh_token"]`, 1), nil},
		{"no code response", strings.Replace(vscodeClientMetadata, responses, `"response_types":["token","id_token"]`, 1), nil},
		{"empty lists", strings.Replace(strings.Replace(vscodeClientMetadata, grants, `"grant_types":[]`, 1), responses, `"response_types":[]`, 1), both},
		{"missing lists", strings.Replace(strings.Replace(vscodeClientMetadata, grants+",", "", 1), responses+",", "", 1), both},
		{"duplicate supported types", strings.Replace(strings.Replace(vscodeClientMetadata, grants, `"grant_types":["refresh_token","authorization_code","refresh_token"]`, 1), responses, `"response_types":["code","code","token"]`, 1), both},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newOAuthHarness(t)
			// Preserve the public client ID and verified domain while fetching the
			// document from a hermetic fixture. Production SSRF checks stay intact.
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.Path != "/oauth/client-metadata.json" {
					t.Error("unexpected metadata request", r.Method, r.URL.Path)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, tc.document)
			}))
			t.Cleanup(server.Close)
			transport := server.Client().Transport.(*http.Transport).Clone()
			t.Cleanup(transport.CloseIdleConnections)
			h.auth.cimdClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				copy := req.Clone(req.Context())
				u, _ := url.Parse(server.URL)
				u.Path = req.URL.Path
				copy.URL = u
				return transport.RoundTrip(copy)
			})}
			client, err := h.auth.clientFor(context.Background(), id)
			registration := h.request("POST", "/oauth/register", tc.document, "application/json", "", "", nil)
			if tc.grants == nil {
				if !errors.Is(err, project.ErrInvalid) {
					t.Fatal("incompatible metadata document accepted", client, err)
				}
				if registration.Code != 400 || oauthDecode[map[string]any](t, registration)["error"] != "invalid_client_metadata" {
					t.Fatal("incompatible registration accepted", registration.Code, registration.Body.String())
				}
				return
			}
			if err != nil || client.ID != id || client.Domain != "vscode.dev" || !slices.Equal(client.GrantTypes, tc.grants) || !slices.Equal(client.ResponseTypes, []string{"code"}) {
				t.Fatal("metadata types were not normalized", client, err)
			}
			// Exercise the original failing browser path, including VS Code's
			// loopback callback on a different ephemeral port.
			query := url.Values{"client_id": {id}, "redirect_uri": {"http://127.0.0.1:45123/"}, "response_type": {"code"}, "state": {"vscode-state"}, "code_challenge": {agentRandom()}, "code_challenge_method": {"S256"}}
			consent := h.request("GET", "/oauth/authorize?"+query.Encode(), "", "", "", "", nil)
			if consent.Code != 200 || !strings.Contains(consent.Body.String(), "Visual Studio Code") {
				t.Fatal("metadata client could not authorize", consent.Code, consent.Body.String())
			}
			if registration.Code != 201 {
				t.Fatal("registration failed", registration.Code, registration.Body.String())
			}
			registered := oauthDecode[project.AgentClient](t, registration)
			if registered.ID == id || !slices.Equal(registered.GrantTypes, tc.grants) || !slices.Equal(registered.ResponseTypes, []string{"code"}) || strings.Contains(registration.Body.String(), "application_type") {
				t.Fatal("registration response was not normalized", registration.Body.String())
			}
			stored, err := h.auth.clientFor(context.Background(), registered.ID)
			if err != nil || !slices.Equal(stored.GrantTypes, tc.grants) || !slices.Equal(stored.ResponseTypes, []string{"code"}) {
				t.Fatal("registration stored unsupported types", stored, err)
			}
		})
	}
}
