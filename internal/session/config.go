package session

import (
	"fmt"
	"strings"
)

// ValidateEnvironment prevents SDK emulator variables silently disabling token
// signature verification in a production process. Demo project IDs cannot refer
// to a real Firebase project. Call before constructing any Firebase SDK client.
func ValidateEnvironment(environment, project, firestoreEmulator, authEmulator string) error {
	if firestoreEmulator == "" && authEmulator == "" {
		return nil
	}
	if environment != "development" || !strings.HasPrefix(project, "demo-") {
		return fmt.Errorf("Firebase emulators require DROP_ENV=development and a demo- project ID")
	}
	if firestoreEmulator == "" || authEmulator == "" {
		return fmt.Errorf("development runtime requires both Firestore and Auth emulator endpoints")
	}
	return nil
}
