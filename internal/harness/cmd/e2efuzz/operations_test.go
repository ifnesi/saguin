package main

import (
	"net/http"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
)

// design.md, Operations, HTTP: "Each answer under /v1/operations/ is an
// object"; "Each grant contains role, kind (channel, topic or broker),
// subject, verbs and denies, the last two lists, [] when empty";
// "Each publish limit is {\"value\":…,\"source\":…}".
// Every planted answer below is valid JSON; it still breaks that shape.
func TestACLAnswerShapeIsChecked(t *testing.T) {
	const answer = `{"user":"ann","acl_file":"/acl.yaml","pattern_applied":"ann","patterns_matched":["ann"],"patterns_in_file":["ann"],"grants":[{"role":"reader","kind":"topic","subject":"a/#","verbs":["read"],"denies":[]}],"grants_withheld":[],"client_id_allowed":null,"publish_rate":{"value":null,"source":"broker"},"publish_bytes":{"value":null,"source":"broker"}}`
	d := &opsDraw{method: http.MethodGet, path: "/v1/operations/acl?user=ann"}
	for _, tc := range []struct {
		name, body string
		valid      bool
	}{
		{"configured", answer, true},
		{"ignores the run's acl_file", `{"acl_file":null,"everything_allowed":true}`, false},
		{"list", `[]`, false},
		{"null", `null`, false},
		{"empty object", `{}`, false},
		{"missing field", strings.Replace(answer, `"user":"ann",`, "", 1), false},
		{"null grant list", strings.Replace(answer, `"denies":[]`, `"denies":null`, 1), false},
		{"wrong grant kind", strings.Replace(answer, `"kind":"topic"`, `"kind":"other"`, 1), false},
		{"object grant kind", strings.Replace(answer, `"kind":"topic"`, `"kind":{}`, 1), false},
		{"wrong limit", strings.Replace(answer, `"value":null`, `"value":"unlimited"`, 1), false},
		{"missing limit value", strings.Replace(answer, `"value":null,`, "", 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &harness.OpsAnswer{Status: http.StatusOK, MediaType: "application/json", Body: tc.body}
			if err := checkOps(d, []int{http.StatusOK}, a); (err == nil) != tc.valid {
				t.Fatalf("ACL answer %s: %v; valid = %v", tc.body, err, tc.valid)
			}
		})
	}
}
