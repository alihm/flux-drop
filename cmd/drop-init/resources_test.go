package main

import (
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestEffectiveLimitsIncludeAncestorsAndUnlimited(t *testing.T) {
	root := t.TempDir()
	child := filepath.Join(root, "child")
	os.Mkdir(child, 0700)
	os.WriteFile(filepath.Join(root, "memory.max"), []byte("1073741824"), 0600)
	os.WriteFile(filepath.Join(child, "memory.max"), []byte("max"), 0600)
	os.WriteFile(filepath.Join(root, "cpu.max"), []byte("150000 100000"), 0600)
	os.WriteFile(filepath.Join(child, "cpu.max"), []byte("200000 100000"), 0600)
	m, c := effectiveLimits([]cgroupMount{{root: "/", mount: root, path: "/child", v2: true}})
	if m != 1<<30 || c != 1.5 {
		t.Fatal(m, c)
	}
	os.WriteFile(filepath.Join(child, "memory.max"), []byte("536870912"), 0600)
	m, _ = effectiveLimits([]cgroupMount{{root: "/", mount: root, path: "/child", v2: true}})
	if m != 512<<20 {
		t.Fatal(m)
	}
	if dirs := ancestorDirs(cgroupMount{root: "/private", mount: root, path: "/elsewhere"}); len(dirs) != 0 {
		t.Fatal("escaped cgroup mount")
	}
}
func TestRuntimeSizingAndMemorySplit(t *testing.T) {
	workers, connections, e := nginxSizing(8, 1.5, 65536)
	if e != nil || workers != 2 || connections != 8192 {
		t.Fatal(workers, connections, e)
	}
	_, connections, e = nginxSizing(4, math.Inf(1), 1024)
	if e != nil || connections >= 1024 {
		t.Fatal(connections, e)
	}
	if _, _, e = nginxSizing(4, 4, 128); e == nil {
		t.Fatal("unusable inherited nofile")
	}
	drop, cluster := memoryBudget(1<<30, true)
	if drop+cluster > int64(1<<30)*65/100 || cluster == 0 {
		t.Fatal("duplicate container memory budgets", drop, cluster)
	}
	if drop, cluster = memoryBudget(0, true); drop != 0 || cluster != 0 {
		t.Fatal("invented memory limit")
	}
}
