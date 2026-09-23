package project

import "cloud.google.com/go/firestore"

// Charges are conservative until replica-aware GC can prove reclamation.
// The per-operation floor also bounds the number of tiny retained versions.
func versionCharge(bytes int64) int64 {
	if bytes < 1<<20 {
		return 1 << 20
	}
	return bytes
}

func (s *FirestoreRepository) byteLimit(o Owner) int64 {
	if o.Kind == "anonymous" {
		if s.AnonymousByteLimit > 0 {
			return s.AnonymousByteLimit
		}
		return 1 << 30
	}
	if s.AccountByteLimit > 0 {
		return s.AccountByteLimit
	}
	return 10 << 30
}

func (s *FirestoreRepository) readQuota(tx *firestore.Transaction, o Owner) (quota, error) {
	q, err := read[quota](tx, s.ref("quotas", ownerKey(o)))
	if missing(err) {
		return quota{Schema: 1}, err
	}
	if err == nil && (q.Schema != 1 || q.Count < 0 || q.ChargedBytes < 0) {
		return quota{}, ErrConflict
	}
	return q, err
}

func fitsBytes(current, extra, limit int64) bool {
	return current >= 0 && extra > 0 && current <= limit && extra <= limit-current
}
