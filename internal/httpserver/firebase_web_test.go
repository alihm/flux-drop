package httpserver

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFirebaseWebConfiguration(t *testing.T) {
	env := map[string]string{"FIREBASE_PROJECT_ID": "demo-drop", "DROP_FIREBASE_WEB_API_KEY": strings.Repeat("a", 30), "DROP_FIREBASE_WEB_APP_ID": "1:123:web:abcdef"}
	get := func(k string) string { return env[k] }
	c, err := FirebaseWebFromEnv(get)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	HomePageWithAuth(c).ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	policy := w.Header().Get("Content-Security-Policy")
	if !strings.Contains(policy, "frame-src https://demo-drop.firebaseapp.com;") || strings.Contains(policy, "unsafe-") || strings.Contains(policy, "script-src 'self'") {
		t.Fatal(policy)
	}
	if strings.Count(strings.ToLower(w.Body.String()), "</script>") != 1 {
		t.Fatal("bundle can terminate inline script")
	}
	c.AuthDomain = "evil.example"
	if c.Validate() == nil {
		t.Fatal("accepted unrelated auth domain")
	}
	delete(env, "DROP_FIREBASE_WEB_APP_ID")
	if _, err = FirebaseWebFromEnv(get); err == nil {
		t.Fatal("accepted partial config")
	}
	delete(env, "DROP_FIREBASE_WEB_API_KEY")
	if c, err = FirebaseWebFromEnv(get); err != nil || c != nil {
		t.Fatal("custom project must not inherit unrelated defaults", c, err)
	}
	delete(env, "FIREBASE_PROJECT_ID")
	if c, err = FirebaseWebFromEnv(get); err != nil || c == nil || c.ProjectID != "fluxcore-prod" || c.AuthDomain != "fluxcore-prod.firebaseapp.com" || c.APIKey == "" || c.AppID == "" {
		t.Fatal("default config", c, err)
	}
	env["DROP_FIREBASE_WEB_API_KEY"] = strings.Repeat("b", 30)
	env["DROP_FIREBASE_WEB_APP_ID"] = "1:456:web:abcdef"
	if c, err = FirebaseWebFromEnv(get); err != nil || c.APIKey != env["DROP_FIREBASE_WEB_API_KEY"] || c.AppID != env["DROP_FIREBASE_WEB_APP_ID"] {
		t.Fatal("overrides ignored", c, err)
	}
}
