// drop-browser applies an inherited filesystem and syscall sandbox before
// executing Chromium. Page assets arrive over Playwright's pipe, so the browser
// needs no network sockets and cannot read primary data, credentials, or jobs.
package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "browser sandbox:", err)
		os.Exit(1)
	}
}
func run() error {
	job := os.Getenv("DROP_BROWSER_JOB")
	if !strings.HasPrefix(job, "/tmp/drop-browser-") || strings.Contains(job, "..") || strings.Contains(job[5:], "/") {
		return fmt.Errorf("invalid browser job directory")
	}
	runtime.LockOSThread()
	abi, _, errno := unix.Syscall(unix.SYS_LANDLOCK_CREATE_RULESET, 0, 0, unix.LANDLOCK_CREATE_RULESET_VERSION)
	if errno != 0 || abi < 1 {
		return fmt.Errorf("Landlock unavailable: %v", errno)
	}
	handled := uint64(0x1fff)
	if abi >= 2 {
		handled |= unix.LANDLOCK_ACCESS_FS_REFER
	}
	if abi >= 3 {
		handled |= unix.LANDLOCK_ACCESS_FS_TRUNCATE
	}
	ruleset, _, errno := unix.Syscall(unix.SYS_LANDLOCK_CREATE_RULESET, uintptr(unsafe.Pointer(&handled)), 8, 0)
	if errno != 0 {
		return errno
	}
	defer unix.Close(int(ruleset))
	read := uint64(unix.LANDLOCK_ACCESS_FS_READ_FILE | unix.LANDLOCK_ACCESS_FS_READ_DIR | unix.LANDLOCK_ACCESS_FS_EXECUTE)
	allow := func(path string, rights uint64, optional bool) error {
		fd, err := unix.Open(path, unix.O_PATH|unix.O_CLOEXEC, 0)
		if os.IsNotExist(err) && optional {
			return nil
		}
		if err != nil {
			return err
		}
		defer unix.Close(fd)
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			rights &= unix.LANDLOCK_ACCESS_FS_READ_FILE | unix.LANDLOCK_ACCESS_FS_WRITE_FILE | unix.LANDLOCK_ACCESS_FS_EXECUTE | unix.LANDLOCK_ACCESS_FS_TRUNCATE
		}
		// landlock_path_beneath_attr is a packed 12-byte structure.
		attr := make([]byte, 12)
		binary.LittleEndian.PutUint64(attr, rights)
		binary.LittleEndian.PutUint32(attr[8:], uint32(fd))
		_, _, errno := unix.Syscall6(unix.SYS_LANDLOCK_ADD_RULE, ruleset, 1, uintptr(unsafe.Pointer(&attr[0])), 0, 0, 0)
		if errno != 0 {
			return errno
		}
		return nil
	}
	for _, path := range []string{"/usr", "/lib", "/lib64", "/etc/fonts", "/etc/ld.so.cache", "/etc/localtime", "/proc/cpuinfo", "/proc/meminfo", "/proc/stat", "/proc/uptime", "/proc/self/stat", "/proc/self/status", "/proc/self/maps", "/proc/self/fd", "/proc/sys/fs/inotify/max_user_watches", "/proc/sys/kernel/random/boot_id"} {
		if err := allow(path, read, true); err != nil {
			return err
		}
	}
	if err := allow(job, handled, false); err != nil {
		return err
	}
	for _, path := range []string{"/dev/null", "/dev/urandom", "/dev/random"} {
		if err := allow(path, unix.LANDLOCK_ACCESS_FS_READ_FILE|unix.LANDLOCK_ACCESS_FS_WRITE_FILE, false); err != nil {
			return err
		}
	}
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return err
	}
	if _, _, errno = unix.Syscall(unix.SYS_LANDLOCK_RESTRICT_SELF, ruleset, 0, 0); errno != 0 {
		return errno
	}
	if err := networkFilter(); err != nil {
		return err
	}
	_ = unix.Setrlimit(unix.RLIMIT_CORE, &unix.Rlimit{Cur: 0, Max: 0})
	_ = unix.Setrlimit(unix.RLIMIT_FSIZE, &unix.Rlimit{Cur: 256 << 20, Max: 256 << 20})
	if len(os.Args) == 2 && os.Args[1] == "--verify-sandbox" {
		if _, err := os.ReadDir("/data"); err == nil {
			return fmt.Errorf("primary data is readable")
		}
		if _, err := os.ReadDir("/var/lib/drop-cluster"); err == nil {
			return fmt.Errorf("primary state is readable")
		}
		if fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0); err == nil {
			unix.Close(fd)
			return fmt.Errorf("network socket allowed")
		}
		if fd, err := unix.Socket(unix.AF_UNIX, unix.SOCK_STREAM, 0); err != nil {
			return err
		} else {
			unix.Close(fd)
		}
		fmt.Println("PASS: filesystem and network sandbox")
		return nil
	}
	return unix.Exec("/usr/lib/chromium/chromium", append([]string{"chromium"}, os.Args[1:]...), os.Environ())
}
func networkFilter() error {
	arch := uint32(unix.AUDIT_ARCH_X86_64)
	if runtime.GOARCH == "arm64" {
		arch = unix.AUDIT_ARCH_AARCH64
	} else if runtime.GOARCH != "amd64" {
		return fmt.Errorf("unsupported browser architecture")
	}
	filters := []unix.SockFilter{{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 4}, {Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: arch, Jt: 1}, {Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_KILL_PROCESS}, {Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 0}}
	// Reject alternate x32 syscall numbers before comparing native syscalls.
	if runtime.GOARCH == "amd64" {
		filters = append(filters, unix.SockFilter{Code: unix.BPF_JMP | unix.BPF_JSET | unix.BPF_K, K: 0x40000000, Jf: 1}, unix.SockFilter{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_KILL_PROCESS})
	}
	for _, number := range []uint32{unix.SYS_CONNECT, unix.SYS_PTRACE, unix.SYS_PROCESS_VM_READV, unix.SYS_PROCESS_VM_WRITEV, unix.SYS_PIDFD_GETFD, unix.SYS_PIDFD_SEND_SIGNAL, unix.SYS_KILL, unix.SYS_TKILL, unix.SYS_TGKILL, unix.SYS_RT_SIGQUEUEINFO, unix.SYS_RT_TGSIGQUEUEINFO, unix.SYS_TRUNCATE, unix.SYS_IO_URING_SETUP, unix.SYS_IO_URING_ENTER, unix.SYS_IO_URING_REGISTER, unix.SYS_BPF, unix.SYS_OPEN_BY_HANDLE_AT} {
		filters = append(filters, unix.SockFilter{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: number, Jf: 1}, unix.SockFilter{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | uint32(unix.EPERM)})
	}
	filters = append(filters,
		unix.SockFilter{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: unix.SYS_SOCKET, Jf: 3},
		unix.SockFilter{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 16},
		unix.SockFilter{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: unix.AF_UNIX, Jt: 1},
		unix.SockFilter{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | uint32(unix.EPERM)},
		unix.SockFilter{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ALLOW})
	program := unix.SockFprog{Len: uint16(len(filters)), Filter: &filters[0]}
	_, _, errno := unix.Syscall(unix.SYS_SECCOMP, unix.SECCOMP_SET_MODE_FILTER, unix.SECCOMP_FILTER_FLAG_TSYNC, uintptr(unsafe.Pointer(&program)))
	if errno != 0 {
		return errno
	}
	return nil
}
