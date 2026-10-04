package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/ifnesi/saguin/internal/broker"
	"os"
	"slices"
	"strings"

	"golang.org/x/term"

	"github.com/ifnesi/saguin/internal/passwd"
)

// User management lives in the broker's own binary, because saguin ships as
// one file and "install this other tool to add a user" is not something a
// single binary gets to say. It is the same argument as `--licenses`: what
// an operator needs must be in the thing they were given.
//
// The verbs are mosquitto_passwd's, so that somebody who has managed a
// Mosquitto fleet already knows this: add a user, delete a user, and a
// password given on the command line or prompted for. `list` and `scope` are
// the two additions: `list` because a hashed file cannot be read by eye, and
// `scope` because only the operations file names the routes a user reaches.
//
// Like a migration, this consults no configuration. It reads and writes the
// file it is given, which is what makes it usable when the configuration is
// the thing that is wrong - and it lets a file be prepared on another
// machine, before there is a broker to run it.
func managePasswd(args []string, withConfig, withCheck bool) int {
	if withConfig {
		return refuse("--config cannot be given with --passwd. It reads and writes the file you\n" +
			"name and consults no configuration; accepting one would imply that it respects it.")
	}
	if withCheck {
		return refuse("--check-config and --passwd cannot be given together: one validates a\n" +
			"configuration and the other writes credentials.")
	}
	if len(args) < 2 {
		return refuse(passwdUsage)
	}

	action, path := args[0], args[1]
	rest := args[2:]

	switch action {
	case "list":
		if len(rest) != 0 {
			return refuse("--passwd list takes only the file:\n\n    saguin --passwd list <file>")
		}
		f, err := passwd.Load(path)
		if err != nil {
			return refuse(err.Error())
		}
		users := f.Users()
		if len(users) == 0 {
			fmt.Println("(no users)")
			return 0
		}
		// **A column only where the file has scopes**, so a password file
		// nobody has narrowed - which is every MQTT one - lists exactly as
		// it always did, one name per line and nothing appearing out of
		// nowhere.
		if !f.AnyScoped() {
			for _, u := range users {
				fmt.Println(u)
			}
			return 0
		}
		width := 0
		for _, u := range users {
			width = max(width, len(u))
		}
		for _, u := range users {
			fmt.Printf("%-*s  %s\n", width, u, reaches(f.Scopes(u)))
		}
		return 0

	case "scope":
		if len(rest) != 2 {
			return refuse("--passwd scope takes a file, a user, and the routes it may reach:\n\n" +
				"    saguin --passwd scope <file> <user> /metrics,/v1/operations\n" +
				"    saguin --passwd scope <file> <user> all\n\n" +
				"It sets the whole list rather than adding to it: a user with no routes\n" +
				"named reaches every one, so an \"add\" against such a user would have to\n" +
				"narrow it, which is an add that removes.")
		}
		user, want := rest[0], rest[1]
		f, err := passwd.Load(path)
		if err != nil {
			return refuse(err.Error())
		}
		var scopes []string
		if want != "all" {
			for _, one := range strings.Split(want, ",") {
				one = strings.TrimSpace(one)
				if !slices.Contains(passwd.KnownRoutes, one) {
					return refuse(fmt.Sprintf(
						"%s is not a route saguin serves. It serves:\n\n    %s",
						one, strings.Join(passwd.KnownRoutes, "\n    ")))
				}
				scopes = append(scopes, one)
			}
		}
		was := reaches(f.Scopes(user))
		if !f.SetScopes(user, scopes) {
			return refuse(fmt.Sprintf("%s is not in %s", user, path))
		}
		if err := f.Save(); err != nil {
			return refuse(err.Error())
		}
		// **What it was, on the line below.** Setting the whole list is one
		// meaning rather than two, and the cost is that somebody who meant
		// to add a route and typed only the new one has removed the others.
		// This is where they see it, rather than when a scraper starts
		// failing.
		fmt.Printf("%s now reaches %s\n  was %s\n", user, reaches(scopes), was)
		return 0

	case "add":
		if len(rest) < 1 || len(rest) > 2 {
			return refuse("--passwd add takes a file, a user, and optionally the password:\n\n" +
				"    saguin --passwd add <file> <user> [password]\n\n" +
				"With no password it is prompted for, which keeps it out of your shell history.")
		}
		user := rest[0]
		if strings.ContainsAny(user, ":\n") {
			return refuse(fmt.Sprintf("a user name cannot hold a colon or a newline: %q\n"+
				"The file is one user per line, and the colon is what separates the name from the hash.", user))
		}
		if !broker.ValidName(user) {
			return refuse(fmt.Sprintf("a user name cannot hold a NUL or a control character: %q\n"+
				"Neither door takes such a name as anybody's, so this user could never authenticate.", user))
		}
		password := ""
		if len(rest) == 2 {
			password = rest[1]
		} else {
			var err error
			if password, err = prompt(user); err != nil {
				return refuse(err.Error())
			}
		}
		if password == "" {
			return refuse("an empty password is not a password. Nothing in saguin refuses to " +
				"hash one,\nwhich is exactly why this does.")
		}

		// A file that does not exist yet is the first user, which is the
		// ordinary way one of these is started.
		f, err := passwd.Load(path)
		if errors.Is(err, os.ErrNotExist) {
			f = passwd.New(path)
		} else if err != nil {
			return refuse(err.Error())
		}
		had := alreadyListed(f.Users(), user)
		if err := f.Set(user, password); err != nil {
			return refuse(err.Error())
		}
		if err := f.Save(); err != nil {
			return refuse(err.Error())
		}
		if had {
			fmt.Printf("changed the password for %s in %s\n", user, path)
			// **Said out loud, because the alternative is a doubt.** A
			// password and a scope are separate commands, so rotating a
			// credential leaves the routes alone - and somebody who has
			// just narrowed a user should not have to reopen the file to
			// find out whether they undid it.
			if sc := f.Scopes(user); sc != nil {
				fmt.Printf("  %s still reaches %s\n", user, reaches(sc))
			}
		} else {
			fmt.Printf("added %s to %s\n", user, path)
		}
		return 0

	case "delete":
		if len(rest) != 1 {
			return refuse("--passwd delete takes a file and a user:\n\n" +
				"    saguin --passwd delete <file> <user>")
		}
		f, err := passwd.Load(path)
		if err != nil {
			return refuse(err.Error())
		}
		if !f.Delete(rest[0]) {
			// Not an error: the file ends up in the state that was asked
			// for either way, and a script removing a user that has already
			// gone has not failed at anything.
			fmt.Printf("%s is not in %s\n", rest[0], path)
			return 0
		}
		if err := f.Save(); err != nil {
			return refuse(err.Error())
		}
		fmt.Printf("deleted %s from %s\n", rest[0], path)
		return 0
	}
	return refuse(passwdUsage)
}

const passwdUsage = "--passwd manages a password file:\n\n" +
	"    saguin --passwd list   <file>\n" +
	"    saguin --passwd add    <file> <user> [password]\n" +
	"    saguin --passwd delete <file> <user>\n" +
	"    saguin --passwd scope  <file> <user> <routes|all>\n\n" +
	"The file is Mosquitto's format, so an existing one can be used as it stands.\n" +
	"`scope` adds a third field naming the routes a user may reach, which only the\n" +
	"operations password file has: an MQTT one has no routes."

// reaches says what a scope list comes to, in the words the commands print.
func reaches(scopes []string) string {
	if len(scopes) == 0 {
		return "every route"
	}
	return strings.Join(scopes, ", ")
}

// prompt reads a password from the terminal twice, with the echo turned
// off.
//
// **Off, or the password is on the screen and in whatever scrollback,
// screen share or CI log is watching.** The terminal's state is restored
// whatever happens, including on the interrupt that a prompt invites:
// leaving somebody's shell with echo disabled is a broken terminal they
// have to work out how to fix.
//
// Twice, because a typo in a password nobody can see locks out whoever it
// was for, and they find out at the next connection rather than now.
func prompt(user string) (string, error) {
	if !isTerminal(os.Stdin.Fd()) {
		// A pipe is not a terminal and has no echo to turn off. Reading the
		// password from it is right - that is how a script does this - but
		// it is read once, because there is nobody to type it twice.
		line, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if err != nil && line == "" {
			return "", fmt.Errorf("reading the password from standard input: %w", err)
		}
		return strings.TrimRight(line, "\r\n"), nil
	}

	first, err := readNoEcho(fmt.Sprintf("password for %s: ", user))
	if err != nil {
		return "", err
	}
	again, err := readNoEcho("again: ")
	if err != nil {
		return "", err
	}
	if first != again {
		return "", errors.New("the two do not match, and nothing was written")
	}
	return first, nil
}

// readNoEcho asks once, with the terminal not repeating what is typed.
//
// **`golang.org/x/term` rather than a direct ioctl**, because the ioctl this
// needs is not the same number on every platform: Linux is `TCGETS`/
// `TCSETS`, Darwin and the BSDs are `TIOCGETA`/`TIOCSETA`. saguin is tested
// primarily on Fedora, where the Linux-only constants this used to call
// compiled and worked - and failed to build at all anywhere else. The
// library carries that platform difference so this file does not have to.
func readNoEcho(ask string) (string, error) {
	fd := int(os.Stdin.Fd())
	fmt.Fprint(os.Stderr, ask)
	pw, err := term.ReadPassword(fd)
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return "", fmt.Errorf("reading the password: %w", err)
	}
	return strings.TrimRight(string(pw), "\r\n"), nil
}

func isTerminal(fd uintptr) bool {
	return term.IsTerminal(int(fd))
}

// alreadyListed says whether a user is in the file, so that adding one and
// changing a password can say which of the two just happened.
func alreadyListed(all []string, want string) bool {
	for _, s := range all {
		if s == want {
			return true
		}
	}
	return false
}
