package harness

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
)

// JSONObject reads an object with exactly the named keys. Missing fields,
// null and additional fields are different answers, even when valid JSON.
func JSONObject(body []byte, keys ...string) (map[string]any, error) {
	var object map[string]any
	if err := json.Unmarshal(body, &object); err != nil {
		return nil, err
	}
	want := slices.Clone(keys)
	slices.Sort(want)
	if object == nil || !slices.Equal(slices.Sorted(maps.Keys(object)), want) {
		return nil, fmt.Errorf("JSON keys %v; want exactly %v", slices.Sorted(maps.Keys(object)), want)
	}
	return object, nil
}

// ACLFields are design.md, Operations, HTTP's fields: "Otherwise it
// contains user, acl_file, pattern_applied, patterns_matched,
// patterns_in_file, grants, grants_withheld, client_id_allowed, publish_rate
// and publish_bytes."
var ACLFields = []string{"user", "acl_file", "pattern_applied", "patterns_matched", "patterns_in_file",
	"grants", "grants_withheld", "client_id_allowed", "publish_rate", "publish_bytes"}

// CheckACLAnswer checks an answer with an acl_file, from design.md, Operations,
// HTTP: "Each grant contains role, kind (channel, topic or broker),
// subject, verbs and denies, the last two lists, [] when empty";
// "Each publish limit is {\"value\":…,\"source\":…}".
func CheckACLAnswer(body []byte) error {
	var a map[string]any
	if err := json.Unmarshal(body, &a); err != nil {
		return err
	}
	for _, field := range ACLFields {
		if _, ok := a[field]; !ok {
			return fmt.Errorf("ACL answer lacks %s", field)
		}
	}
	isString := func(v any) bool { _, ok := v.(string); return ok }
	stringsOnly := func(v any) bool {
		list, ok := v.([]any)
		return ok && !slices.ContainsFunc(list, func(v any) bool { return !isString(v) })
	}
	_, clientBool := a["client_id_allowed"].(bool)
	if !isString(a["user"]) || !isString(a["acl_file"]) ||
		(a["pattern_applied"] != nil && !isString(a["pattern_applied"])) ||
		(a["client_id_allowed"] != nil && !clientBool) ||
		!stringsOnly(a["patterns_matched"]) || !stringsOnly(a["patterns_in_file"]) {
		return fmt.Errorf("invalid ACL identity or pattern fields: %s", body)
	}
	for _, field := range []string{"grants", "grants_withheld"} {
		grants, ok := a[field].([]any)
		if !ok {
			return fmt.Errorf("ACL %s is not a list", field)
		}
		for _, value := range grants {
			g, ok := value.(map[string]any)
			kind, stringKind := g["kind"].(string)
			if !ok || !stringKind || !isString(g["role"]) || !isString(g["subject"]) ||
				!slices.Contains([]string{"channel", "topic", "broker"}, kind) ||
				!stringsOnly(g["verbs"]) || !stringsOnly(g["denies"]) {
				return fmt.Errorf("invalid ACL %s entry: %v", field, value)
			}
		}
	}
	for _, field := range []string{"publish_rate", "publish_bytes"} {
		limit, ok := a[field].(map[string]any)
		if !ok || len(limit) != 2 || !isString(limit["source"]) {
			return fmt.Errorf("invalid ACL %s: %v", field, a[field])
		}
		value, exists := limit["value"]
		_, number := value.(float64)
		if !exists || (value != nil && !number) {
			return fmt.Errorf("invalid ACL %s value: %v", field, value)
		}
	}
	return nil
}
