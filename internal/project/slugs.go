package project

// chooseSlug runs inside the reservation/rename transaction. Old aliases and
// deleted-project tombstones stay occupied; concurrent callers cannot take them.
func chooseSlug(name, seed, id string, lookup func(string) (string, error)) (string, string, error) {
	candidates := []int{0, 6, 10, 16, 32, 64}
	for _, length := range candidates {
		suffix, slug := "", name
		if length != 0 {
			if len(seed) < length {
				continue
			}
			suffix = seed[:length]
			slug += "-" + suffix
		}
		if !slugRE.MatchString(slug) {
			return "", "", ErrInvalid
		}
		switch slug {
		case "api", "admin", "agents", "unlock", "healthz", "readyz":
			continue
		}
		owner, err := lookup(slug)
		if err != nil {
			return "", "", err
		}
		if owner == "" || owner == id {
			return slug, suffix, nil
		}
	}
	return "", "", ErrConflict
}
