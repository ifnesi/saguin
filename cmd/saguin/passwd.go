package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/term"

	"github.com/ifnesi/saguin/internal/operations/routes"
	"github.com/ifnesi/saguin/internal/passwd"
)

const passwdUsage = "usage: saguin --passwd list <file>\n" +
	"       saguin --passwd add <file> <user> [password]\n" +
	"       saguin --passwd delete <file> <user>\n" +
	"       saguin --passwd scope <file> <user> <routes or all>"

// passwdCommand manages a file independently of the broker's configuration.
// Usage errors exit 2; refused reads, edits, writes and prompts exit 1.
func passwdCommand(args []string, stdout, stderr io.Writer) int {
	valid := false
	if len(args) >= 2 && args[1] != "" {
		switch args[0] {
		case "list":
			valid = len(args) == 2
		case "add":
			valid = len(args) == 3 || len(args) == 4
		case "delete":
			valid = len(args) == 3
		case "scope":
			valid = len(args) == 4
		}
	}
	if !valid {
		_, _ = fmt.Fprintln(stderr, passwdUsage)
		return 2
	}
	if err := editPasswords(args, stdout, stderr); err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func editPasswords(args []string, stdout, stderr io.Writer) error {
	verb, name := args[0], args[1]
	data, err := os.ReadFile(name)
	if verb == "add" && errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	if err != nil {
		return err
	}
	// Standalone edits accept either password-file role; only startup knows
	// which configured files belong to clients and forbids scopes in those.
	file, err := passwd.ParseOperators(name, data, routes.Paths[:])
	if err != nil {
		return err
	}
	if verb == "list" {
		users := file.Users()
		if len(users) == 0 {
			users = []string{"(no users)"}
		}
		_, err := fmt.Fprintln(stdout, strings.Join(users, "\n"))
		return err
	}
	user := args[2]
	switch verb {
	case "delete":
		if !file.Delete(user) {
			_, err := fmt.Fprintf(stdout, "%s is not listed in %s\n", user, name)
			return err
		}
	case "scope":
		if err := file.SetScope(user, args[3]); err != nil {
			return err
		}
	default:
		var password string
		if len(args) == 4 {
			password = args[3]
		} else {
			password, err = promptPassword(stderr)
			if err != nil {
				return err
			}
		}
		if err := file.Set(user, password); err != nil {
			return err
		}
	}
	return file.Save()
}

// promptPassword always asks twice on the terminal. ReadPassword owns the
// echo change and restores it before returning, including on a read error.
func promptPassword(stderr io.Writer) (string, error) {
	var first string
	for i, prompt := range [...]string{"Password: ", "Repeat password: "} {
		if _, err := io.WriteString(stderr, prompt); err != nil {
			return "", err
		}
		answer, err := term.ReadPassword(int(os.Stdin.Fd()))
		if _, writeErr := fmt.Fprintln(stderr); err == nil {
			err = writeErr
		}
		if err != nil {
			return "", fmt.Errorf("read password: %w", err)
		}
		if i == 0 {
			first = string(answer)
		} else if string(answer) != first {
			return "", errors.New("passwords do not match")
		}
	}
	return first, nil
}
