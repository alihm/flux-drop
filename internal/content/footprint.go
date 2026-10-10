package content

import (
	"encoding/json"
	"path"
)

// Footprint is a persistent filesystem allowance, excluding upload spools and
// installation scratch space. Directory entries are budgeted from actual paths,
// rather than assuming twenty new directories for every file.
type Footprint struct {
	ContentBytes, AllocatedBytes int64
	Inodes                       uint64
}

func (m Manifest) Footprint(block int64) (Footprint, error) {
	if m.Schema != 1 || block < 1 || block > 1<<20 || len(m.Files) == 0 || len(m.Files) > 5000 {
		return Footprint{}, ErrInvalid
	}
	round := func(n int64) int64 { return (n + block - 1) / block * block }
	dirs := map[string]int64{"": 64, "public": 0} // public directory + manifest entry
	f := Footprint{}
	for _, entry := range m.Files {
		if validateFile(entry.Path) != nil || entry.Size < 0 || entry.Size > 200<<20 {
			return Footprint{}, ErrInvalid
		}
		f.ContentBytes += entry.Size
		if f.ContentBytes > 200<<20 {
			return Footprint{}, ErrLimit
		}
		f.AllocatedBytes += round(entry.Size)
		name := "public/" + entry.Path
		dirs[path.Dir(name)] += int64(len(path.Base(name)) + 32)
		for d := path.Dir(name); d != "public"; d = path.Dir(d) {
			if _, exists := dirs[d]; !exists {
				dirs[d] = 0
			}
		}
	}
	// Count every directory entry once, even where many files share ancestors.
	for dir := range dirs {
		if dir != "" && dir != "public" {
			dirs[path.Dir(dir)] += int64(len(path.Base(dir)) + 32)
		}
	}
	for _, size := range dirs {
		if size < 1 {
			size = 1
		}
		f.AllocatedBytes += round(size)
	}
	encoded, err := json.Marshal(m)
	if err != nil {
		return Footprint{}, err
	}
	f.AllocatedBytes += round(int64(len(encoded)))
	// Shared project marker and parent directories are charged conservatively
	// per version; this is a few blocks, rather than a fixed multi-MiB allowance.
	f.AllocatedBytes += 4 * block
	f.Inodes = uint64(len(m.Files) + len(dirs) + 5)
	return f, nil
}
