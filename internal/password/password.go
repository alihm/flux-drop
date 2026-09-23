// Package password implements bounded password hashing, not access authorization.
package password

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"unicode/utf8"

	"golang.org/x/crypto/argon2"
)

var ErrInvalid = errors.New("invalid password or password record")
var ErrBusy = errors.New("password worker capacity exhausted")

// Parameters are fixed for schema 1. Never honor attacker-supplied memory or
// iteration settings. Future upgrades require an explicitly supported schema.
const memoryKiB = 64 * 1024
const iterations = 2

type Hash struct {
	Schema    int    `json:"schema"`
	Algorithm string `json:"algorithm"`
	Salt      string `json:"salt"`
	Key       string `json:"key"`
}

type Hasher struct{ slots chan struct{} }

func NewHasher() *Hasher { return &Hasher{slots: make(chan struct{}, 2)} }

func validPassword(value string) bool {
	return len(value) <= 1024 && utf8.ValidString(value) && utf8.RuneCountInString(value) >= 12
}
func (h *Hasher) acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case h.slots <- struct{}{}:
		return nil
	default:
		return ErrBusy
	}
}
func (h *Hasher) Create(ctx context.Context, value string) (Hash, error) {
	if !validPassword(value) {
		return Hash{}, ErrInvalid
	}
	if err := h.acquire(ctx); err != nil {
		return Hash{}, err
	}
	defer func() { <-h.slots }()
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return Hash{}, err
	}
	key := argon2.IDKey([]byte(value), salt, iterations, memoryKiB, 1, 32)
	if err := ctx.Err(); err != nil {
		return Hash{}, err
	}
	return Hash{1, "argon2id", base64.RawStdEncoding.EncodeToString(salt), base64.RawStdEncoding.EncodeToString(key)}, nil
}
func (r Hash) validate() ([]byte, []byte, error) {
	if r.Schema != 1 || r.Algorithm != "argon2id" || len(r.Salt) != 22 || len(r.Key) != 43 {
		return nil, nil, ErrInvalid
	}
	salt, err := base64.RawStdEncoding.Strict().DecodeString(r.Salt)
	if err != nil || len(salt) != 16 {
		return nil, nil, ErrInvalid
	}
	key, err := base64.RawStdEncoding.Strict().DecodeString(r.Key)
	if err != nil || len(key) != 32 {
		return nil, nil, ErrInvalid
	}
	return salt, key, nil
}
func (h *Hasher) Verify(ctx context.Context, value string, record Hash) (bool, error) {
	if !validPassword(value) {
		return false, ErrInvalid
	}
	salt, want, err := record.validate()
	if err != nil {
		return false, err
	}
	if err := h.acquire(ctx); err != nil {
		return false, err
	}
	defer func() { <-h.slots }()
	got := argon2.IDKey([]byte(value), salt, iterations, memoryKiB, 1, 32)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return subtle.ConstantTimeCompare(got, want) == 1, nil
}
