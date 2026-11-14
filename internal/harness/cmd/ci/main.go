// Command ci runs one command under the whole-suite CI time budget. The
// command gets a process group of its own so a timeout or terminal interrupt
// cannot leave grandchildren running after this supervisor returns.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

const shutdownGrace = 500 * time.Millisecond

func main() {
	os.Exit(run(os.Args[1:], os.Stderr))
}

func run(args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("ci", flag.ContinueOnError)
	flags.SetOutput(stderr)
	budget := flags.Duration("budget", 5*time.Minute, "time allowed for the whole command")
	started := flags.Int64("started", 0, "Unix nanoseconds when the enclosing recipe started")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	command := flags.Args()
	if *budget <= 0 || len(command) == 0 {
		_, _ = fmt.Fprintln(stderr, "ci: a positive -budget and a command are required")
		return 2
	}

	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(interrupts)

	cmd := exec.Command(command[0], command[1:]...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_, _ = fmt.Fprintf(stderr, "ci: starting suite: %v\n", err)
		return 1
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	remaining := *budget
	if *started != 0 {
		remaining = time.Until(time.Unix(0, *started).Add(*budget))
	}
	if remaining <= 0 {
		stopGroup(cmd.Process.Pid)
		<-done
		_, _ = fmt.Fprintf(stderr, "ci: exceeded CI_BUDGET of %v\n", *budget)
		return 124
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()

	select {
	case err := <-done:
		// A stage can exit while a grandchild it started remains. Kill the
		// now-orphaned group before reporting the stage's result.
		stopGroup(cmd.Process.Pid)
		return exitCode(err)
	case <-timer.C:
		stopGroup(cmd.Process.Pid)
		<-done
		_, _ = fmt.Fprintf(stderr, "ci: exceeded CI_BUDGET of %v\n", *budget)
		return 124
	case sig := <-interrupts:
		stopGroup(cmd.Process.Pid)
		<-done
		if value, ok := sig.(syscall.Signal); ok {
			return 128 + int(value)
		}
		return 1
	}
}

func stopGroup(pid int) {
	// A negative pid addresses the process group created for the suite. Give
	// stages and test brokers a chance to clean up before forcing survivors
	// down; the bounded grace keeps the supervisor's own deadline meaningful.
	if err := syscall.Kill(-pid, syscall.SIGTERM); errors.Is(err, syscall.ESRCH) {
		return
	}
	timer := time.NewTimer(shutdownGrace)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer timer.Stop()
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := syscall.Kill(-pid, 0); errors.Is(err, syscall.ESRCH) {
				return
			}
		case <-timer.C:
			_ = syscall.Kill(-pid, syscall.SIGKILL)
			return
		}
	}
}

func exitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return 1
	}
	if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
		if status.Signaled() {
			return 128 + int(status.Signal())
		}
		return status.ExitStatus()
	}
	return 1
}
