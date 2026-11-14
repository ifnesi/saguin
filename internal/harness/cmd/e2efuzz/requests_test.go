package main

import (
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/ifnesi/saguin/internal/harness"
)

// The fake broker's kv/get, catalogue and sessions/disconnect (design.md,
// $saguin topics; requests.go): it answers each as the design has the
// broker answer it, on the request's Response Topic, which every one the
// runner sends carries.

// serviceRequest answers p, on topic from c holding id's session, where it
// is a kv/get, a catalogue request or a disconnect, and reports whether it
// was one (handled) and whether the connection goes on. It runs under the
// lock (owns).
func (b *fake) serviceRequest(c *fakeConn, id, topic string, p *harness.Publish) (handled, goesOn bool) {
	asked := string(p.Payload)
	var reason byte
	var answer func() (payload, fallback string)
	switch name, catalogue := strings.CutPrefix(topic, "$saguin/catalogue/"); {
	case topic == kvGetTopic:
		// "The broker refuses a topic on no latest channel, with a wildcard,
		// under $, or over max_topic_length or max_topic_levels with 0x90. It
		// refuses an empty payload with 0x83."; "It needs read on that topic
		// (else 0x87)."
		ch, _ := b.fakeRoute(asked)
		switch {
		case !b.fakeMay(c.user, asked, false):
			reason = 0x87
		case asked == "":
			reason = 0x83
		case strings.ContainsAny(asked, "+#") || strings.HasPrefix(asked, "$") || len(asked) > maxTopicLength ||
			strings.Count(asked, "/") >= maxTopicLevels || ch < 2 || fakeConfigured[ch-2].Type != "latest":
			reason = 0x90
		}
		if p.QoS == 0 {
			return true, true // "At QoS 0, the request is dropped."
		}
		answer = func() (string, string) {
			if r := b.latest[asked]; r != nil {
				return string(r.payload), asked
			}
			return "", asked
		}
	case catalogue:
		if p.QoS == 0 {
			return true, true // "At QoS 0, the request is dropped."
		}
		answer = func() (string, string) { return b.describe(c.user, name), name }
	case topic == disconnectTopic:
		target := b.sessions[asked]
		switch {
		case b.fault != ignoredACL && c.user == restricted:
			reason = 0x87 // "The request needs sessions: disconnect (else 0x87)."
		case asked == "" || asked == id:
			reason = 0x83 // "The broker refuses an empty payload or the asker's own id with 0x83."
		}
		answer = func() (string, string) {
			if target == nil || target.conn == nil || target.conn.finished {
				return "no-such-client", ""
			}
			// "An operator disconnects the client whose id is the payload (0x98).
			// Its session is kept." End its ownership and settle its Will in
			// this locked step, before queuing the request's answer. A Will
			// delivery can therefore precede that answer on the requester's
			// connection, without relying on the old reader's scheduling.
			hungUp := target.conn
			if hungUp.version == harness.V5 {
				hungUp.send(&harness.Disconnect{Reason: 0x98})
			}
			hungUp.finish()
			b.release(asked, hungUp)
			b.lose(asked, hungUp)
			return "hung-up", ""
		}
	default:
		return false, true
	}
	if reason != 0 && p.QoS == 0 {
		return true, true // "A refusal at QoS 0 is dropped."
	}
	if !b.acknowledgeRequest(c, p, reason) {
		return true, false
	}
	if reason != 0 {
		return true, true
	}
	payload, fallback := answer()
	out := &harness.Publish{Payload: []byte(payload)}
	rt, _ := p.Props.Get(harness.ResponseTopic)
	out.Topic = rt.Str
	// "The answer carries the request's Correlation Data, or the topic where
	// it set none or an empty one."
	switch cd, ok := p.Props.Get(harness.CorrelationData); {
	case b.fault == uncorrelated:
	case ok && (len(cd.Data) > 0 || fallback == ""):
		out.Props = harness.Properties{cd}
	case fallback != "":
		out.Props = harness.Properties{{ID: harness.CorrelationData, Data: []byte(fallback)}}
	}
	b.answer(c, out)
	return true, true
}

// describe is the catalogue of the channel named as user asks it: "" for a
// channel that does not exist or holds none of user's verbs, else
// {"name","type","filter","verbs"}, "the filter as configured and the verbs
// of the asker's grants that name the channel", and a queue's "pin": every
// grant of user's naming the channel, wherever its filter reaches. The
// verbs are writeACL's: its rules' write, read and consume, and seek on all
// but a latest channel for every user but restricted.
func (b *fake) describe(user, name string) string {
	i := slices.IndexFunc(fakeConfigured, func(o harness.Channel) bool { return o.Name == name })
	if i < 0 {
		return ""
	}
	ch := fakeConfigured[i]
	verbs := map[string]bool{}
	for _, g := range grantsOf(user) {
		if g.channel != name {
			continue
		}
		for verb, held := range map[string]bool{"write": g.write, "read": g.read, "consume": g.consume, "seek": user != restricted && ch.Type != "latest"} {
			if held {
				verbs[verb] = true
			}
		}
	}
	if len(verbs) == 0 {
		return ""
	}
	d := map[string]any{"name": name, "type": ch.Type, "filter": ch.Filter, "verbs": slices.Sorted(maps.Keys(verbs))}
	if ch.Type == "queue" {
		d["pin"] = "$saguin/queue/" + name
	}
	switch b.fault {
	case extraCatalogueKey:
		d["extra"] = "not in the design"
	case extraCataloguePin:
		d["pin"] = "not a queue"
	}
	wire, _ := json.Marshal(d) // of strings alone
	return string(wire)
}

const (
	extraCatalogueKey fault = "extra catalogue key"
	extraCataloguePin fault = "pin on an append catalogue"
)

// design.md, $saguin topics: "describes a channel:
// {\"name\":…,\"type\":…,\"filter\":…,\"verbs\":[…]}"; "a queue adds
// \"pin\", its worker topic $saguin/queue/<channel>". A fake's extra field
// must fail the same answer checker that runs against the broker.
func TestCatalogueKeysAreChecked(t *testing.T) {
	for _, fault := range []fault{extraCatalogueKey, extraCataloguePin} {
		t.Run(string(fault), func(t *testing.T) {
			caughtInScene(t, fault, sceneQueue, "JSON keys", func(sc *scene) error {
				s, err := sc.client(0, "cy", false, false)
				if err != nil {
					return err
				}
				p, a := s.newRequest(catalogueTopic("ca"), "", true, "ca")
				p.QoS = 1
				a.check = catalogueOf(s.user, "ca").check
				return s.send(p, a, "CATALOGUE")
			})
		})
	}
}

// design.md, Flows: "Policy checks select the first applicable reason in
// the stated order, independently of property order." The kv/get row's "It
// needs read on that topic (else 0x87)." precedes its 0x90 topic checks.
// These topics violate both checks for the restricted user.
func TestKVGetChecksReadBeforeTopic(t *testing.T) {
	err := playScene(t, "", sceneQueue, func(sc *scene) error {
		s, err := sc.client(0, restricted, false, false)
		if err != nil {
			return err
		}
		for _, topic := range []string{"ca/no-right", "$SYS/x", "outside/x"} {
			p, a := s.newRequest(kvGetTopic, topic, true, topic)
			p.QoS = 1
			a.refuse(kvGetReason(s.user, topic), p.QoS)
			if err := s.send(p, a, "KV/GET"); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
