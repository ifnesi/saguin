package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/ifnesi/saguin/internal/channel"
	"github.com/ifnesi/saguin/internal/config"
)

// outputForm is how an inspect command prints: columns under a header for a
// person, JSON for a program. Named rather than a bare bool because it is
// the last argument of two commands and `explainACL(args, in, out, bad,
// true, false)` says nothing at the call site about which `false` is which.
type outputForm int

const (
	asColumns outputForm = iota
	asJSON
)

// **JSON is one object per line, not one array**, for the reason the column
// form has the input in its first column: a line stands on its own however
// the output is cut about. An array has to be buffered whole before it is
// valid, so a run over a fleet's worth of topics says nothing until it has
// finished all of them, and `grep` stops working on it. `jq` reads a stream
// of objects without being asked to; `jq -s` collects them into an array
// for anybody who wanted one.
//
// HTML escaping is off because these strings are MQTT topics and filters,
// and a topic holding `&` is not going anywhere near a browser.
func jsonLines(out io.Writer) *json.Encoder {
	enc := json.NewEncoder(out)
	enc.SetEscapeHTML(false)
	return enc
}

// orNull is the JSON half of the column form's `-`: a column with no answer
// in it is null rather than an empty string, so that a consumer testing
// `.channel == null` cannot be fooled by a channel that is genuinely named
// the empty string - which the name rules forbid, but a test that does not
// depend on them is one fewer thing to keep true.
func orNull(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// dash is the other half: what the column form writes where JSON writes
// null. RFC 0002 defines it to mean "nothing here", which is why a `-` must
// never be printed for a case that has an answer.
func dash(p *string) string {
	if p == nil {
		return "-"
	}
	return *p
}

// routeTopic answers the question a filter makes hard to answer by reading:
// **where does this topic go, and what would this filter be served?**
//
// A channel used to claim its own name, so the answer was the first level
// of the topic and nobody needed a command for it. A channel now carries a
// filter, filters overlap on purpose, and which one holds a topic is
// settled by a rule - so the answer is worked out rather than read off, and
// an operator should not have to do it in their head at the point where a
// device that should be working is not.
//
// **It calls the functions the broker calls.** Resolve is the publish
// path's, ResolveFilters is what SUBSCRIBE asks, and the queue answer is
// the one that issues the reason codes. A second implementation here would
// be a claim nobody ran - it would agree with the broker on the day it was
// written and drift silently afterwards, which is the failure this command
// exists to prevent rather than to reproduce.
//
// Two writers, as `--acl explain` has: an explanation is what somebody
// pipes, and a refusal is what a script watching standard error looks for.
func routeTopic(args []string, in io.Reader, out, bad io.Writer, piped bool, form outputForm) int {
	if len(args) == 0 || len(args) > 2 {
		fmt.Fprintln(bad, "usage: saguin --route <config-file> [topic-or-filter]")
		fmt.Fprintln(bad, "  a topic answers which channel holds it; a filter answers "+
			"what a subscriber would be served")
		fmt.Fprintln(bad, "  with neither, one is read per line from standard input")
		return 1
	}
	cfgPath := args[0]

	_, reg, err := config.Load(cfgPath)
	if err != nil {
		fmt.Fprintf(bad, "%v\n", err)
		return 1
	}

	// **Nothing to ask about means a list on standard input**, one per line.
	// The case this is for is a fleet's topics against a configuration
	// somebody has just edited - "which of these four hundred still land
	// where they did" is a diff of two runs, and a paragraph each is not.
	if len(args) == 1 {
		if !piped {
			fmt.Fprintln(bad, "no topic or filter given and nothing piped in.")
			fmt.Fprintln(bad, "  saguin --route <config-file> <topic-or-filter>   one, explained")
			fmt.Fprintln(bad, "  saguin --route <config-file> < topics.txt        one per line, a line each")
			return 1
		}
		return routeBatch(reg, in, out, form)
	}
	subject := args[1]

	// **The JSON form of one subject is the same answer the list form
	// gives**, plus the routing table the paragraph prints below. A second
	// shape here would be a second thing to keep true, and the paragraph is
	// already the form for a person: what JSON is for is the script that
	// wants the fields, and those are the fields.
	if form == asJSON {
		kind := "topic"
		if asksAboutASubscription(subject) {
			kind = "filter"
		}
		filters, chans := reg.Routes()
		table := make([]routeTableRow, 0, len(filters))
		for i, f := range filters {
			table = append(table, routeTableRow{
				Filter: f, Channel: chans[i].Name, Type: string(chans[i].Type),
			})
		}
		if err := jsonLines(out).Encode(routeExplanation{
			Subject:      subject,
			Config:       cfgPath,
			Kind:         kind,
			Reaches:      routeReachesFor(reg, subject),
			RoutingTable: table,
		}); err != nil {
			fmt.Fprintf(bad, "%v\n", err)
			return 1
		}
		return 0
	}

	fmt.Fprintf(out, "%q, against %s\n\n", subject, cfgPath)

	if asksAboutASubscription(subject) {
		explainOneFilter(out, reg, subject)
	} else {
		explainOneTopic(out, reg, subject)
	}

	// The table itself, in the order resolution walks it, because "why that
	// one" is the next question and it is answered by seeing what came
	// first. It is the same slice Resolve walks.
	fmt.Fprintln(out, "\nthe routing table, most exact first:")
	filters, chans := reg.Routes()
	for i, f := range filters {
		fmt.Fprintf(out, "  %-40s %s (%s)\n", f, chans[i].Name, chans[i].Type)
	}
	return 0
}

// routeBatch answers a list of topics and filters, one per line in, with
// one tab-separated line per answer out.
//
// **Four columns, and the first is always the input.** A line stands on its
// own however the output is cut about, which is the whole point of the
// shape: `awk -F'\t' '$3 == "broadcast"'` is every topic no channel claims,
// and a diff of two runs against two configurations is every topic a filter
// edit moved.
//
// The fourth column answers "and so?" and is therefore not the same fact in
// both cases, deliberately. For a topic it is the filter that claimed it,
// which is *why* it landed there and the line an operator goes and edits.
// For a filter it is what a subscriber would actually be served, which is
// the thing they are about to be surprised by.
//
// A filter reaching several channels is several lines. That is not a
// failure to summarise: it reaches several channels, and one line saying so
// would have to invent a summary of three different delivery semantics.
//
// **A header line names the columns**, for the reason `--acl`'s does: this
// form exists because a person wants one screen, and bare columns make them
// count tabs to find out which one they are reading. The fourth is called
// `why` rather than anything more exact because it is deliberately two
// different facts - the filter that claimed a topic, or what a subscriber
// would be served - and a name that fitted one would be a lie about the
// other. It starts with `#` so the rule below skips it: an operator asking
// the same question again cuts the first column and pipes it back, and the
// header must not become one more question. Its column names carry no `/`
// for the same reason, that being what tells a comment from the `#` filter
// - so renaming one to `topic/filter` would have this command answer its
// own header in the middle of the next run.
func routeBatch(reg *channel.Registry, in io.Reader, out io.Writer, form outputForm) int {
	enc := jsonLines(out)
	if form == asColumns {
		fmt.Fprintf(out, "# topic-or-filter\tchannel\ttype\twhy\n")
	}
	s := bufio.NewScanner(in)
	// Comfortably past limits.max_topic_length at any setting an operator
	// would choose; without it a long line is reported as end of input.
	s.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for s.Scan() {
		subject := strings.TrimSpace(s.Text())
		// A bare `#` is a legal filter and a comment marker is not, so the
		// two are told apart by there being nothing else on the line: `#`
		// asks about everything, `# note` is a note.
		//
		// **Nothing else starting with `#` can be a question.** MQTT allows
		// the multi-level wildcard only as a whole filter on its own, and a
		// topic a client publishes to may not hold a wildcard at all - so
		// every other line beginning with `#` is a comment or a mistake,
		// and answering it is wrong either way. This used to also require
		// the line to hold no `/`, which let exactly the comments this
		// input is most likely to carry through: `# my/fleet topics` was
		// answered as a topic, and duly reported as broadcast.
		if subject == "" || (strings.HasPrefix(subject, "#") && subject != "#") {
			continue
		}
		if err := writeRouteAnswers(out, enc, form, routeAnswers(reg, subject)); err != nil {
			return 1
		}
	}
	return boolExit(s.Err() == nil)
}

// writeRouteAnswers renders answers already worked out. **One computation,
// two renderings**, so that the columns and the JSON cannot become two
// accounts of where a topic lands - which is the drift this command exists
// to prevent rather than to reproduce.
func writeRouteAnswers(out io.Writer, enc *json.Encoder, form outputForm, rows []routeAnswer) error {
	for _, r := range rows {
		if form == asJSON {
			if err := enc.Encode(r); err != nil {
				return err
			}
			continue
		}
		if _, err := fmt.Fprintf(out, "%s\t%s\t%s\t%s\n",
			r.Subject, dash(r.Channel), dash(r.Type), dash(r.Why)); err != nil {
			return err
		}
	}
	return nil
}

// asksAboutASubscription reports whether a subject is a question about a
// SUBSCRIBE rather than about a publish.
//
// A wildcard is the obvious half. **The two prefixes are the half that is
// easy to miss**, and getting it wrong is silent: a worker's pin carries no
// `+` or `#`, so a wildcard test alone calls `$saguin/queue/jobs` a topic
// and answers "broadcast" about the one subscription a queue admits. That
// is the wrong answer to the question somebody is most likely asking this
// command: why was my worker turned away. `$share/` is the same shape - a
// shared subscription over a spelled-out topic has no wildcard either.
//
// A plain literal string with none of the three is answered as a topic. It
// is a legal filter too - one matching only itself - but "where does this
// land" is what an operator typing a bare topic wants, and the filter
// answer for it says the same thing at more length.
func asksAboutASubscription(subject string) bool {
	return strings.HasPrefix(subject, "$share/") ||
		strings.HasPrefix(subject, channel.QueuePrefix) ||
		strings.ContainsAny(subject, "+#")
}

// routeExplanation is `--route <config> <subject> --json`: what the list
// form would say about that one subject, and the routing table the
// paragraph form prints under its answer.
type routeExplanation struct {
	Subject      string          `json:"subject"`
	Config       string          `json:"config"`
	Kind         string          `json:"kind"`
	Reaches      []routeReach    `json:"reaches"`
	RoutingTable []routeTableRow `json:"routing_table"`
}

// routeReach is one thing a subject reaches: the same three answer fields
// the list form carries, without the subject, which the object around them
// already names once. **Derived from routeAnswers rather than worked out
// again**, so there is still one account of where a topic lands.
type routeReach struct {
	Channel *string `json:"channel"`
	Type    *string `json:"type"`
	Why     *string `json:"why"`
}

// routeReachesFor is that derivation.
func routeReachesFor(reg *channel.Registry, subject string) []routeReach {
	out := []routeReach{}
	for _, a := range routeAnswers(reg, subject) {
		out = append(out, routeReach{Channel: a.Channel, Type: a.Type, Why: a.Why})
	}
	return out
}

// routeTableRow is one line of the routing table, in the order resolution
// walks it - most exact first, which is what makes "why that one" readable.
type routeTableRow struct {
	Filter  string `json:"filter"`
	Channel string `json:"channel"`
	Type    string `json:"type"`
}

// routeAnswer is one answer to one subject, worked out and not yet
// written. The four fields are the four columns, and a nil is the `-` the
// column form prints and the `null` the JSON form does.
type routeAnswer struct {
	Subject string  `json:"subject"`
	Channel *string `json:"channel"`
	Type    *string `json:"type"`
	Why     *string `json:"why"`
}

// routeAnswers works out every answer for one subject: one for a topic, and
// a line per channel for a filter that reaches several.
//
// It calls the functions the broker calls - Resolve is the publish path's,
// ResolveFilters is what SUBSCRIBE asks, and QueueSubscriptionError is what
// issues the reason codes - so the answer is the broker's rather than a
// second implementation of it.
func routeAnswers(reg *channel.Registry, subject string) []routeAnswer {
	if !asksAboutASubscription(subject) {
		c := reg.Resolve(subject)
		if c == nil {
			return []routeAnswer{{Subject: subject, Type: orNull("broadcast")}}
		}
		return []routeAnswer{{
			Subject: subject,
			Channel: orNull(c.Name),
			Type:    orNull(string(c.Type)),
			Why:     orNull(c.Filter),
		}}
	}

	if q, why := reg.QueueSubscriptionError(subject, 1); q != nil {
		reason := "worker"
		if why != "" {
			reason = "refused-0x8F"
		}
		return []routeAnswer{{
			Subject: subject,
			Channel: orNull(q.Name),
			Type:    orNull("queue"),
			Why:     orNull(reason),
		}}
	} else if why != "" {
		// **A refusal naming no queue**, which is a spelling under
		// `$saguin/queue/` that no queue answers to. It has no channel to
		// print, and answering it through the routing table below would call
		// it broadcast - the one answer that reads as "this works".
		return []routeAnswer{{Subject: subject, Why: orNull("refused-0x8F")}}
	}
	// **A shared subscription is served live and nothing else**, on every
	// channel type, so the words for what it gets are not the ordinary
	// ones: no replay from a position, no current-state pass, and nothing
	// at all from a queue.
	shared := strings.HasPrefix(subject, "$share/")

	var rows []routeAnswer
	served := 0
	for _, c := range reg.ResolveFilters(channel.InnerFilter(subject)) {
		what := map[channel.Type]string{
			channel.Append: "replayed",
			channel.Latest: "current",
			channel.Queue:  "excluded",
		}[c.Type]
		if shared {
			what = map[channel.Type]string{
				channel.Append: "live-shared",
				channel.Latest: "live-shared",
				channel.Queue:  "excluded",
			}[c.Type]
		}
		if c.Type != channel.Queue {
			served++
		}
		rows = append(rows, routeAnswer{
			Subject: subject,
			Channel: orNull(c.Name),
			Type:    orNull(string(c.Type)),
			Why:     orNull(what),
		})
	}
	if served == 0 {
		rows = append(rows, routeAnswer{
			Subject: subject,
			Type:    orNull("broadcast"),
			Why:     orNull("live"),
		})
	}
	return rows
}

// boolExit turns "it worked" into the exit code the shell reads.
func boolExit(ok bool) int {
	if ok {
		return 0
	}
	return 1
}

// explainOneTopic answers a publish: one channel, or broadcast.
func explainOneTopic(out io.Writer, reg *channel.Registry, topic string) {
	c := reg.Resolve(topic)
	if c == nil {
		fmt.Fprintln(out, "  broadcast - no channel's filter matches it, so it is delivered")
		fmt.Fprintln(out, "  live to whoever is subscribed and stored nowhere")
		return
	}
	fmt.Fprintf(out, "  channel %q (%s), whose filter is %q\n", c.Name, c.Type, c.Filter)
	switch c.Type {
	case channel.Append:
		fmt.Fprintln(out, "  stored with an offset, replayed to a consumer from its own position")
	case channel.Latest:
		fmt.Fprintln(out, "  kept as this topic's current value, replacing whatever it held")
	case channel.Queue:
		fmt.Fprintf(out, "  work, offered to one worker at a time through %s\n", c.QueueFilter())
		fmt.Fprintf(out, "  a job that runs out of attempts moves to %q, at %q\n",
			c.DLQ.Name, channel.DLQTopic(c.Filter, topic))
	}
}

// explainOneFilter answers a SUBSCRIBE: every channel it touches, and what
// a fresh subscriber would be served from each.
func explainOneFilter(out io.Writer, reg *channel.Registry, filter string) {
	inner := channel.InnerFilter(filter)

	// The queue first, because it is the one answer that is a refusal
	// rather than a delivery, and because the code the broker sends is what
	// an operator is actually looking at when they run this.
	if q, why := reg.QueueSubscriptionError(filter, 1); q != nil {
		if why == "" {
			fmt.Fprintf(out, "  a worker of queue %q - one job at a time, to one worker,\n", q.Name)
			fmt.Fprintf(out, "  answered on %s\n", q.ResponseTopic())
			return
		}
		fmt.Fprintf(out, "  refused SUBACK 0x8F (Topic Filter invalid): %s\n", why)
		return
	} else if why != "" {
		fmt.Fprintf(out, "  refused SUBACK 0x8F (Topic Filter invalid): %s\n", why)
		return
	}
	shared := strings.HasPrefix(filter, "$share/")

	reached := reg.ResolveFilters(inner)
	var served int
	for _, c := range reached {
		switch c.Type {
		case channel.Append:
			served++
			fmt.Fprintf(out, "  channel %q (append), filter %q\n", c.Name, c.Filter)
			if shared {
				fmt.Fprintln(out, "    live records only, split across this shared group - no replay,")
				fmt.Fprintln(out, "    no offsets, and no stored position")
				continue
			}
			fmt.Fprintln(out, "    replayed from this consumer's position, or from the channel's")
			fmt.Fprintln(out, "    retention floor when it has none, then live")
		case channel.Latest:
			served++
			fmt.Fprintf(out, "  channel %q (latest), filter %q\n", c.Name, c.Filter)
			if shared {
				fmt.Fprintln(out, "    changes only, split across this shared group - no pass of")
				fmt.Fprintln(out, "    current state on subscribe")
				continue
			}
			fmt.Fprintln(out, "    the current value of every matching topic, carrying RETAIN,")
			fmt.Fprintln(out, "    then changes as they happen")
		case channel.Queue:
			fmt.Fprintf(out, "  channel %q (queue), filter %q\n", c.Name, c.Filter)
			fmt.Fprintln(out, "    crossed but not served: nothing but a worker reaches a queue,")
			fmt.Fprintf(out, "    and its records go only to %s\n", c.QueueFilter())
		}
	}
	if served == 0 {
		fmt.Fprintln(out, "  broadcast only - live traffic on the topics it matches that no")
		fmt.Fprintln(out, "  channel's filter claims, plus any retained value there")
		return
	}
	fmt.Fprintln(out, "  and broadcast, live, for any topic it matches that no filter claims")
}
