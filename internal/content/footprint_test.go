package content

import (
	"strings"
	"testing"
)

func TestFootprintUsesActualPathsAndBlockRounding(t *testing.T) {
	m := Manifest{Schema: 1, Files: []File{{Path: "index.html", Size: 100, SHA256: strings.Repeat("a", 64)}}}
	u, err := m.Footprint(4096)
	if err != nil || u.ContentBytes != 100 || u.AllocatedBytes != 8*4096 || u.Inodes != 8 {
		t.Fatal(u, err)
	}
	m.Files[0].Size = 4097
	bigger, err := m.Footprint(4096)
	if err != nil || bigger.AllocatedBytes-u.AllocatedBytes != 4096 {
		t.Fatal(bigger, err)
	}
	m.Files = append(m.Files, File{Path: "assets/a.css", Size: 12}, File{Path: "assets/b.css", Size: 12})
	shared, err := m.Footprint(4096)
	if err != nil || shared.Inodes != bigger.Inodes+3 {
		t.Fatal("shared directory counted more than once", shared, err)
	}
	m.Files[2].Path = "other/b.css"
	separate, err := m.Footprint(4096)
	if err != nil || separate.AllocatedBytes-shared.AllocatedBytes != 4096 || separate.Inodes != shared.Inodes+1 {
		t.Fatal(separate, err)
	}
}

func TestFootprintRejectsInvalidInput(t *testing.T) {
	for _, m := range []Manifest{{}, {Schema: 1, Files: []File{{Path: "../index.html"}}}, {Schema: 1, Files: []File{{Path: "index.html", Size: -1}}}, {Schema: 1, Files: []File{{Path: "index.html", Size: 201 << 20}}}} {
		if _, err := m.Footprint(4096); err == nil {
			t.Fatal("invalid manifest accepted", m)
		}
	}
	m := Manifest{Schema: 1, Files: []File{{Path: "index.html", Size: 1}}}
	for _, block := range []int64{0, -1, 1 << 21} {
		if _, err := m.Footprint(block); err == nil {
			t.Fatal("invalid block size", block)
		}
	}
}
