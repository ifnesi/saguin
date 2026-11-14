package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/topic"
)

type routeReach struct {
	Channel *string `json:"channel"`
	Type    *string `json:"type"`
	Why     *string `json:"why"`
}

type routeEntry struct {
	Filter  string `json:"filter"`
	Channel string `json:"channel"`
	Type    string `json:"type"`
}

type routeReport struct {
	Subject string       `json:"subject"`
	Config  string       `json:"config"`
	Kind    string       `json:"kind"`
	Reaches []routeReach `json:"reaches"`
	Table   []routeEntry `json:"routing_table"`
}

func channelType(kind config.ChannelType) string {
	return [...]string{"append", "latest", "queue"}[kind]
}

func (r *routeReport) reach(ch *config.Channel, why string) {
	name, kind := "$broadcast", "append"
	if ch != nil {
		name, kind = ch.Name, channelType(ch.Type)
	}
	line := routeReach{Channel: &name, Type: &kind}
	if why != "" {
		line.Why = &why
	}
	r.Reaches = append(r.Reaches, line)
}

// routeCommand uses configuration alone: no storage lock, database, listener
// or broker goroutine is needed to explain the compiled routing policy.
func routeCommand(args []string, stdout, stderr io.Writer) int {
	if len(args) != 2 && (len(args) != 3 || args[2] != "--json") {
		_, _ = fmt.Fprintln(stderr, "usage: saguin --route <file> <topic or filter> [--json]")
		return 2
	}
	cfg, err := config.Load(args[0])
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	subject := args[1]
	policy := config.NewRouting(cfg.Channels)
	report := routeReport{Subject: subject, Config: args[0], Kind: "topic",
		Reaches: []routeReach{}, Table: make([]routeEntry, 0, len(policy.Table))}
	for _, row := range policy.Table {
		ch := &policy.Channels[row.Channel]
		report.Table = append(report.Table, routeEntry{row.Filter, ch.Name, channelType(ch.Type)})
	}
	if strings.ContainsAny(subject, "+#") || strings.HasPrefix(subject, "$share/") || strings.HasPrefix(subject, "$saguin/queue/") {
		report.Kind = "filter"
	}
	code := 0
	if name, worker := strings.CutPrefix(subject, "$saguin/queue/"); worker {
		why := "refused-0x8F"
		line := routeReach{Why: &why}
		for i := range policy.Channels {
			ch := &policy.Channels[i]
			if ch.Name == name && ch.Type == config.Queue {
				kind := "queue"
				line.Channel, line.Type, why = &ch.Name, &kind, "worker"
				break
			}
		}
		report.Reaches = append(report.Reaches, line)
		if why != "worker" {
			code = 1
		}
	} else {
		group, filter, splitErr := topic.SplitShared(subject)
		limits := topic.Limits{MaxLength: cfg.Broker.Limits.MaxTopicLength, MaxLevels: cfg.Broker.Limits.MaxTopicLevels}
		if splitErr != nil || !utf8.ValidString(subject) || strings.ContainsRune(subject, '\x00') || topic.CheckFilter(filter, limits) != nil || topic.CheckFilter(subject, limits) != nil {
			_, _ = fmt.Fprintln(stderr, "invalid route subject: "+subject)
			return 1
		}
		if report.Kind == "topic" {
			if i := policy.Resolve(subject); i >= 0 {
				ch := &policy.Channels[i]
				report.reach(ch, ch.Filter)
			} else {
				report.reach(nil, "")
			}
		} else {
			for _, i := range policy.Reaches(filter) {
				if i < 0 {
					why := "live"
					if group != "" {
						why = "live-shared"
					}
					report.reach(nil, why)
					continue
				}
				ch := &policy.Channels[i]
				why := [...]string{"replayed", "current", "excluded"}[ch.Type]
				if group != "" {
					why = "live-shared"
				}
				report.reach(ch, why)
			}
		}
	}
	if len(args) == 3 {
		err = json.NewEncoder(stdout).Encode(report)
	} else {
		for _, line := range report.Reaches {
			name := "null"
			if line.Channel != nil {
				name = *line.Channel
			}
			if _, err = fmt.Fprintln(stdout, name); err != nil {
				break
			}
		}
	}
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	return code
}
