package content

import (
	"fmt"
	"strings"
)

const maxFileErrors = 20

// FileErrors reports a bounded set of invalid upload paths. It still unwraps to
// ErrInvalid so callers keep their existing status and error-code handling.
type FileErrors struct {
	issues []fileIssue
}

type fileIssue struct {
	path, reason string
}

func (e *FileErrors) Error() string {
	parts := make([]string, 0, len(e.issues))
	for _, issue := range e.issues {
		parts = append(parts, fmt.Sprintf("%s: %q", issue.reason, issue.path))
	}
	return ErrInvalid.Error() + ": " + strings.Join(parts, "; ")
}

func (e *FileErrors) Unwrap() error { return ErrInvalid }

// Paths returns original upload names, including any enclosing ZIP directory.
func (e *FileErrors) Paths() []string {
	paths := make([]string, 0, len(e.issues))
	for _, issue := range e.issues {
		paths = append(paths, issue.path)
	}
	return paths
}

func (e *FileErrors) add(name, reason string) {
	if len(e.issues) >= maxFileErrors {
		return
	}
	for _, issue := range e.issues {
		if issue.path == name {
			return
		}
	}
	e.issues = append(e.issues, fileIssue{path: name, reason: reason})
}

func (e *FileErrors) merge(err error) {
	if other, ok := err.(*FileErrors); ok {
		for _, issue := range other.issues {
			e.add(issue.path, issue.reason)
		}
	}
}

func fileError(name, reason string) *FileErrors {
	e := &FileErrors{}
	e.add(name, reason)
	return e
}
