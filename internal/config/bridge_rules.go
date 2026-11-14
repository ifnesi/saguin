package config

import (
	"errors"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/ifnesi/saguin/internal/topic"
)

// validateBridges waits until all broker limits have been read, regardless
// of where bridges appears in the file. Comparisons stay within one bridge.
func (c *Config) validateBridges() error {
	for i := range c.Bridges {
		b := &c.Bridges[i]
		key := keyPath("bridges", b.Name) + ".topics"
		inbound := make([][]string, len(b.Topics))
		outbound := make([][]string, len(b.Topics))
		peerTopics := make([][]string, len(b.Topics))
		for j := range b.Topics {
			r := &b.Topics[j]
			parent := indexPath(key, j)
			if err := r.validate(parent, &c.Broker.Limits); err != nil {
				return err
			}
			if r.Direction != Out {
				inbound[j] = r.Filters
				if err := checkBridgeOverlap(inbound[:j], r.Filters, key, parent+".filter", "inbound"); err != nil {
					return err
				}
			}
			if r.Direction != In {
				outbound[j] = r.Filters
				if err := checkBridgeOverlap(outbound[:j], r.Filters, key, parent+".filter", "outbound"); err != nil {
					return err
				}
				peerTopics[j] = r.outboundTopicFilters()
				field := ".topic"
				if r.Direction == Both {
					field = ".filter"
				}
				if err := checkBridgeOverlap(peerTopics[:j], peerTopics[j], key, parent+field, "outbound topic"); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// RestoreBridgeRule compiles a stored outbound rule for unfinished exchanges.
// Its original configuration limits are no longer available. MQTT's bounds
// apply to the stored syntax; current broker limits still check each delivery.
func RestoreBridgeRule(filter, template string) (BridgeRule, error) {
	r := BridgeRule{Filter: filter, Topic: template, Direction: Out}
	if template == "" {
		r.Direction = Both
	}
	err := r.validate("stored bridge rule", &Limits{MaxTopicLength: 65535, MaxTopicLevels: 65535})
	return r, err
}

func (r *BridgeRule) validate(key string, limits *Limits) error {
	expanded, err := expandFilter(r.Filter, limits, nil)
	if err != nil {
		return refuse(key+".filter", err.Error())
	}
	for _, filter := range expanded {
		if r.Direction == Both && strings.HasPrefix(filter, "$") {
			return refuse(key+".direction", "both is refused on a $ filter")
		}
		if filter == "$share" || reservedBridgeRoot(filter, r.Direction == Out) {
			return refuse(key+".filter", "reserved bridge filter root")
		}
	}
	r.captures = strings.Count(expanded[0], "+")
	if r.Direction != Both {
		// Alternatives contain no wildcards, so every expansion has the
		// same capture positions. Inspect just the first expansion.
		if err := r.checkTemplate(expanded[0], limits); err != nil {
			return refuse(key+".topic", err.Error())
		}
	}
	r.Filters = expanded
	return nil
}

func reservedBridgeRoot(value string, system bool) bool {
	return topic.HasReservedPrefix(value) || (system && strings.HasPrefix(value, "$SYS/"))
}

// checkTemplate checks and compiles captures in place, without generating a sample
// topic: limits apply to the written template, not a substituted topic.
// topic.CheckName cannot accept $#, and removing it would change the measured
// length and require another syntax pass, so the limits are checked here.
func (r *BridgeRule) checkTemplate(filter string, limits *Limits) error {
	template, direction := r.Topic, r.Direction
	if template == "" || !utf8.ValidString(template) || strings.ContainsRune(template, '\x00') ||
		strings.HasPrefix(template, "/") || strings.HasSuffix(template, "/") ||
		reservedBridgeRoot(template, direction == In) {
		return errors.New("expected a nonempty topic template without an edge '/' or reserved root")
	}
	if len(template) > limits.MaxTopicLength || strings.Count(template, "/")+1 > limits.MaxTopicLevels {
		return errors.New("topic template exceeds limits")
	}
	plus := strings.Count(filter, "+")
	r.template = &bridgeTopic{limits: topic.Limits{MaxLength: limits.MaxTopicLength, MaxLevels: limits.MaxTopicLevels}, uses: make([]int, plus)}
	hash := strings.HasSuffix(filter, "#")
	start := 0
	for i := 0; i < len(template); i++ {
		switch template[i] {
		case '+', '#':
			return errors.New("wildcards are not allowed in a topic template")
		case '$':
			if i+1 == len(template) {
				continue
			}
			if template[i+1] == '#' {
				if !hash || i+2 != len(template) || (i != 0 && template[i-1] != '/') {
					return errors.New("$# requires the filter's # and must be the whole last level")
				}
				r.addTopicPart(template[start:i], -1)
				i++
				start = i + 1
				continue
			}
			end := i + 1
			for end < len(template) && template[end] >= '0' && template[end] <= '9' {
				end++
			}
			if end != i+1 {
				n, err := strconv.Atoi(template[i+1 : end])
				if template[i+1] == '0' || err != nil || n < 1 || n > plus {
					return errors.New("$n must name one of the filter's + levels")
				}
				r.addTopicPart(template[start:i], n)
				i = end - 1
				start = end
			}
		}
	}
	if start < len(template) {
		r.addTopicPart(template[start:], 0)
	}
	return nil
}

// outboundTopicFilters reads validated templates as filters, conservatively
// replacing a whole level containing a numbered capture with +. A common
// leading level makes $ ordinary text in the shared MQTT overlap check.
func (r *BridgeRule) outboundTopicFilters() []string {
	filters := r.Filters
	if r.Direction == Out {
		levels := strings.Split(r.Topic, "/")
		for i, level := range levels {
			if level == "$#" {
				levels[i] = "#"
				continue
			}
			for j := 0; j+1 < len(level); j++ {
				if level[j] == '$' && level[j+1] >= '1' && level[j+1] <= '9' {
					levels[i] = "+"
					break
				}
			}
		}
		return []string{"/" + strings.Join(levels, "/")}
	}
	result := make([]string, len(filters))
	for i, filter := range filters {
		result[i] = "/" + filter
	}
	return result
}

func checkBridgeOverlap(earlier [][]string, filters []string, key, parent, name string) error {
	for i, previous := range earlier {
		for _, first := range previous {
			for _, second := range filters {
				if topic.FiltersOverlap(first, second) {
					return refuse(parent, "overlaps "+name+" rule "+indexPath(key, i))
				}
			}
		}
	}
	return nil
}
