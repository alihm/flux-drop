// Package firebaseconfig holds public Firebase identifiers, never credentials.
package firebaseconfig

// These are the same public web-app defaults used by Orbit.
const (
	ProjectID = "fluxcore-prod"
	APIKey    = "AIzaSyAtMsozWwJhhPIOd9BGkZxk5D6Wr8jVGVM"
	AppID     = "1:468366888401:web:56eb34ebe93751527ea4f0"
)

func ProjectFromEnv(get func(string) string) string {
	if value := get("FIREBASE_PROJECT_ID"); value != "" {
		return value
	}
	return ProjectID
}
