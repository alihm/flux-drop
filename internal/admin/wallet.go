package admin

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"math/big"
	"strings"

	"github.com/decred/dcrd/dcrec/secp256k1/v4/ecdsa"
	"golang.org/x/crypto/ripemd160"
)

const DefaultAddress = "15c3aH6y9Koq1Dg1rGXE9Ypn5nL2AbSJCu"

func addressPayload(address string) []byte {
	if len(address) < 25 || len(address) > 34 || !strings.HasPrefix(address, "1") {
		return nil
	}
	const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	n := new(big.Int)
	for _, c := range address {
		i := strings.IndexRune(alphabet, c)
		if i < 0 {
			return nil
		}
		n.Mul(n, big.NewInt(58))
		n.Add(n, big.NewInt(int64(i)))
	}
	zeros := 0
	for zeros < len(address) && address[zeros] == '1' {
		zeros++
	}
	raw := append(make([]byte, zeros), n.Bytes()...)
	if len(raw) != 25 || raw[0] != 0 {
		return nil
	}
	a := sha256.Sum256(raw[:21])
	b := sha256.Sum256(a[:])
	if subtle.ConstantTimeCompare(raw[21:], b[:4]) != 1 {
		return nil
	}
	return raw[1:21]
}

func messageHash(message string) []byte {
	data := []byte("\x18Bitcoin Signed Message:\n")
	n := len(message)
	if n < 253 {
		data = append(data, byte(n))
	} else {
		data = append(data, 253, 0, 0)
		binary.LittleEndian.PutUint16(data[len(data)-2:], uint16(n))
	}
	data = append(data, []byte(message)...)
	a := sha256.Sum256(data)
	b := sha256.Sum256(a[:])
	return b[:]
}

// VerifyWallet implements the same Bitcoin signed-message contract as Zelcore
// and bitcoinjs-message. Only P2PKH Flux IDs are accepted by this admin flow.
func VerifyWallet(message, address, signature string) bool {
	expected := addressPayload(address)
	if expected == nil || len(message) == 0 || len(message) > 512 || len(signature) != 88 {
		return false
	}
	compact, err := base64.StdEncoding.Strict().DecodeString(signature)
	if err != nil || len(compact) != 65 || compact[0] < 27 || compact[0] > 34 {
		return false
	}
	pub, compressed, err := ecdsa.RecoverCompact(compact, messageHash(message))
	if err != nil {
		return false
	}
	serialized := pub.SerializeUncompressed()
	if compressed {
		serialized = pub.SerializeCompressed()
	}
	a := sha256.Sum256(serialized)
	h := ripemd160.New()
	_, _ = h.Write(a[:])
	return subtle.ConstantTimeCompare(h.Sum(nil), expected) == 1
}
