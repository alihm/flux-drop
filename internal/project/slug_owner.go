package project

import "cloud.google.com/go/firestore"

// A slug record is a reservation only while its project is not deleted. Reading
// both records in the allocator transaction makes name reuse race-safe, releases
// all historical aliases, and handles tombstones written by previous images.
// Missing/malformed projects fail closed rather than handing their names away.
func (s *RaftRepository) slugOwner(tx *raftTx, slug string) (string, error) {
	r, err := raftRead[slugRecord](tx, s.raftRef("slugs", slug))
	if raftMissing(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	p, err := raftRead[Project](tx, s.raftRef("projects", r.ProjectID))
	if err != nil {
		return "", err
	}
	if p.Status == "deleted" {
		return "", nil
	}
	return r.ProjectID, nil
}

func (s *FirestoreRepository) slugOwner(tx *firestore.Transaction, slug string) (string, error) {
	r, err := read[slugRecord](tx, s.ref("slugs", slug))
	if missing(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	p, err := read[Project](tx, s.ref("projects", r.ProjectID))
	if err != nil {
		return "", err
	}
	if p.Status == "deleted" {
		return "", nil
	}
	return r.ProjectID, nil
}
