package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/ifnesi/saguin/internal/acl"
	"github.com/ifnesi/saguin/internal/config"
)

// aclCommand explains disk policy without opening a store or any listeners.
func aclCommand(args []string, stdout, stderr io.Writer) int {
	jsonOutput := len(args) != 0 && args[len(args)-1] == "--json"
	if jsonOutput {
		args = args[:len(args)-1]
	}
	if len(args) < 2 || len(args) > 3 {
		_, _ = fmt.Fprintln(stderr, "usage: saguin --acl <config> <user> [client-id] [--json]")
		return 2
	}
	cfg, err := config.Load(args[0])
	if err == nil {
		err = cfg.LoadACL()
	}
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	var id *string
	if len(args) == 3 {
		id = &args[2]
	}
	if cfg.ACL == nil {
		if jsonOutput {
			err = json.NewEncoder(stdout).Encode(acl.EverythingAllowed{EverythingAllowed: true})
		} else {
			_, err = fmt.Fprintln(stdout, "No acl_file: everything is allowed.")
		}
	} else {
		x := cfg.ACL.Explain(args[1], id, int64(cfg.Broker.Limits.PublishRate), int64(cfg.Broker.Limits.PublishBytes))
		if jsonOutput {
			err = json.NewEncoder(stdout).Encode(x)
		} else {
			err = writeACLExplanation(stdout, &x, id)
		}
	}
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func writeACLExplanation(w io.Writer, x *acl.Explanation, id *string) error {
	var out strings.Builder
	fmt.Fprintf(&out, "User: %s\nacl_file: %s\n", x.User, x.ACLFile)
	entry := "(no entry)"
	if x.PatternApplied != nil {
		entry = *x.PatternApplied
	}
	fmt.Fprintf(&out, "Entry in force: %s\nPatterns matched: %s\n", entry, strings.Join(x.PatternsMatched, ", "))
	if id != nil {
		fmt.Fprintf(&out, "Client id %q allowed: %t\n", *id, *x.ClientIDAllowed)
	}
	for _, l := range []struct {
		name  string
		limit acl.Limit
	}{{"publish_rate", x.PublishRate}, {"publish_bytes", x.PublishBytes}} {
		fmt.Fprintf(&out, "%s: ", l.name)
		if l.limit.Value == nil {
			fmt.Fprint(&out, "none")
		} else {
			fmt.Fprintf(&out, "%d", *l.limit.Value)
		}
		fmt.Fprintf(&out, " (%s)\n", l.limit.Source)
	}
	for _, section := range []struct {
		name   string
		grants []acl.Grant
	}{{"Withheld rules", x.GrantsWithheld}, {"Effective grants", x.Grants}} {
		fmt.Fprintln(&out, section.name+":")
		for _, g := range section.grants {
			fmt.Fprintf(&out, "  role=%s kind=%s subject=%s", g.Role, g.Kind, g.Subject)
			if g.Filter != "" {
				fmt.Fprintf(&out, " filter=%s", g.Filter)
			}
			fmt.Fprintf(&out, " verbs=[%s] denies=[%s]\n", strings.Join(g.Verbs, ", "), strings.Join(g.Denies, ", "))
		}
	}
	_, err := io.WriteString(w, out.String())
	return err
}
