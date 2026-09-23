package httpserver

import (
	"fmt"
	"regexp"

	"github.com/runonflux/flux-drop/internal/firebaseconfig"
)

// FirebaseWebConfig contains public client identifiers, never Admin credentials.
type FirebaseWebConfig struct {
	APIKey     string `json:"apiKey"`
	ProjectID  string `json:"projectId"`
	AuthDomain string `json:"authDomain"`
	AppID      string `json:"appId"`
}

func (c *FirebaseWebConfig) Validate() error {
	if c == nil {
		return nil
	}
	if !regexp.MustCompile(`^[a-z][a-z0-9-]{4,28}[a-z0-9]$`).MatchString(c.ProjectID) || c.AuthDomain != c.ProjectID+".firebaseapp.com" || !regexp.MustCompile(`^[A-Za-z0-9_-]{20,200}$`).MatchString(c.APIKey) || !regexp.MustCompile(`^1:[0-9]+:web:[a-f0-9]+$`).MatchString(c.AppID) {
		return fmt.Errorf("invalid Firebase browser configuration")
	}
	return nil
}

func FirebaseWebFromEnv(get func(string) string) (*FirebaseWebConfig, error) {
	key, app := get("DROP_FIREBASE_WEB_API_KEY"), get("DROP_FIREBASE_WEB_APP_ID")
	projectID := firebaseconfig.ProjectFromEnv(get)
	if projectID == firebaseconfig.ProjectID {
		if key == "" {
			key = firebaseconfig.APIKey
		}
		if app == "" {
			app = firebaseconfig.AppID
		}
	} else if key == "" && app == "" {
		// Preserve API-only/custom-project deployments without browser login.
		return nil, nil
	} else if key == "" || app == "" {
		return nil, fmt.Errorf("custom FIREBASE_PROJECT_ID requires DROP_FIREBASE_WEB_API_KEY and DROP_FIREBASE_WEB_APP_ID")
	}
	c := &FirebaseWebConfig{APIKey: key, ProjectID: projectID, AppID: app}
	c.AuthDomain = c.ProjectID + ".firebaseapp.com"
	return c, c.Validate()
}
