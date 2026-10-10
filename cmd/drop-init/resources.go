package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

type cgroupMount struct {
	root, mount, path string
	v2                bool
	controller        string
}

func cgroupMounts(proc string) []cgroupMount {
	data, _ := os.ReadFile(filepath.Join(proc, "self/cgroup"))
	paths := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.SplitN(line, ":", 3)
		if len(fields) != 3 {
			continue
		}
		if fields[1] == "" {
			paths[""] = fields[2]
		}
		for _, c := range strings.Split(fields[1], ",") {
			paths[c] = fields[2]
		}
	}
	data, _ = os.ReadFile(filepath.Join(proc, "self/mountinfo"))
	var out []cgroupMount
	for _, line := range strings.Split(string(data), "\n") {
		pair := strings.SplitN(line, " - ", 2)
		if len(pair) != 2 {
			continue
		}
		left, right := strings.Fields(pair[0]), strings.Fields(pair[1])
		if len(left) < 5 || len(right) < 3 {
			continue
		}
		unescape := func(v string) string { return strings.NewReplacer(`\040`, " ", `\011`, "\t", `\134`, "\\").Replace(v) }
		root, mount := unescape(left[3]), unescape(left[4])
		if right[0] == "cgroup2" && paths[""] != "" {
			out = append(out, cgroupMount{root, mount, paths[""], true, ""})
		}
		if right[0] == "cgroup" {
			for _, c := range []string{"memory", "cpu"} {
				for _, option := range strings.Split(right[2], ",") {
					if option == c && paths[c] != "" {
						out = append(out, cgroupMount{root, mount, paths[c], false, c})
					}
				}
			}
		}
	}
	return out
}
func ancestorDirs(m cgroupMount) []string {
	path := filepath.Clean(m.path)
	root := filepath.Clean(m.root)
	if path != root && !strings.HasPrefix(path, strings.TrimSuffix(root, "/")+"/") {
		return nil
	}
	rel := strings.TrimPrefix(strings.TrimPrefix(path, root), "/")
	dir := filepath.Join(m.mount, rel)
	var dirs []string
	for {
		dirs = append(dirs, dir)
		if dir == filepath.Clean(m.mount) {
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return nil
		}
		dir = parent
	}
	return dirs
}

// Inspect every visible applicable ancestor. Unreadable/unlimited controllers
// impose no inferred limit. Hidden ancestors cannot safely be guessed.
func effectiveLimits(mounts []cgroupMount) (int64, float64) {
	var memory int64
	cpu := math.Inf(1)
	number := func(path string) int64 {
		data, e := os.ReadFile(path)
		if e != nil {
			return 0
		}
		v, e := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
		if e != nil || v <= 0 || v >= 1<<60 {
			return 0
		}
		return v
	}
	for _, mount := range mounts {
		for _, dir := range ancestorDirs(mount) {
			file := "memory.limit_in_bytes"
			if mount.v2 {
				file = "memory.max"
			}
			if mount.v2 || mount.controller == "memory" {
				n := number(filepath.Join(dir, file))
				if n > 0 && (memory == 0 || n < memory) {
					memory = n
				}
			}
			var quota, period int64
			if mount.v2 {
				data, _ := os.ReadFile(filepath.Join(dir, "cpu.max"))
				fields := strings.Fields(string(data))
				if len(fields) == 2 {
					quota, _ = strconv.ParseInt(fields[0], 10, 64)
					period, _ = strconv.ParseInt(fields[1], 10, 64)
				}
			} else if mount.controller == "cpu" {
				quota = number(filepath.Join(dir, "cpu.cfs_quota_us"))
				period = number(filepath.Join(dir, "cpu.cfs_period_us"))
			}
			if quota > 0 && period > 0 {
				cpu = math.Min(cpu, float64(quota)/float64(period))
			}
		}
	}
	return memory, cpu
}
func memoryBudget(limit int64, coordinator bool) (int64, int64) {
	if limit <= 0 {
		return 0, 0
	}
	if coordinator {
		return limit / 100 * 45, limit / 100 * 20
	}
	return limit / 100 * 55, 0
}
func nginxSizing(affinity int, quota float64, soft uint64) (int, int, error) {
	if affinity < 1 {
		affinity = 1
	}
	workers := affinity
	if !math.IsInf(quota, 1) && quota > 0 && quota < float64(workers) {
		workers = int(math.Ceil(quota))
	}
	if soft < 256 {
		return 0, 0, fmt.Errorf("nginx needs inherited nofile soft limit >=256 (got %d)", soft)
	}
	connections := 8192
	if soft < 16640 {
		connections = int(soft/2) - 128
		if connections < 64 {
			connections = 64
		}
	}
	return workers, connections, nil
}
func configureResources(coordinator bool) (int64, int64, error) {
	memory, quota := effectiveLimits(cgroupMounts("/proc"))
	var cpus unix.CPUSet
	affinity := 1
	if unix.SchedGetaffinity(0, &cpus) == nil {
		affinity = cpus.Count()
	}
	var limit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &limit); err != nil {
		return 0, 0, err
	}
	workers, connections, err := nginxSizing(affinity, quota, limit.Cur)
	if err != nil {
		return 0, 0, err
	}
	data, err := os.ReadFile("/etc/nginx/nginx.conf")
	if err != nil {
		return 0, 0, err
	}
	config := strings.Replace(string(data), "worker_processes auto;", fmt.Sprintf("worker_processes %d;", workers), 1)
	config = strings.Replace(config, "worker_connections 8192", fmt.Sprintf("worker_connections %d", connections), 1)
	if err = os.WriteFile("/tmp/drop-nginx.conf", []byte(config), 0600); err != nil {
		return 0, 0, err
	}
	drop, cluster := memoryBudget(memory, coordinator)
	fmt.Fprintf(os.Stderr, "runtime workers=%d connections=%d nofile=%d/%d memory=%d drop_budget=%d cluster_budget=%d\n", workers, connections, limit.Cur, limit.Max, memory, drop, cluster)
	return drop, cluster, nil
}
