package config

import (
	"strings"

	"github.com/ifnesi/saguin/internal/topic"
)

// bridgeTopic is immutable compiled metadata shared by configuration copies.
type bridgeTopic struct {
	parts                []bridgeTopicPart
	limits               topic.Limits
	literals, separators int
	uses                 []int
	hash                 bool
}

// bridgeTopicPart is compiled by the configuration's template validation.
// Zero is literal text, positive captures name + levels, and -1 names #.
type bridgeTopicPart struct {
	text    string
	capture int
}

// addTopicPart keeps sizing metadata alongside the validated template, so
// sizing a publication visits captures rather than reparsing template text.
func (r *BridgeRule) addTopicPart(text string, capture int) {
	r.template.parts = append(r.template.parts, bridgeTopicPart{text: text, capture: capture})
	r.template.literals += len(text)
	r.template.separators += strings.Count(text, "/")
	if capture > 0 {
		r.template.uses[capture-1]++
	} else if capture == -1 {
		r.template.hash = true
	}
}

// topicSize validates the expanded length and depth without constructing it.
// Division before multiplication bounds arithmetic by the configured limit.
func (r *BridgeRule) topicSize(name string, captures *topic.Captures) (int, bool) {
	size, levels := r.template.literals, r.template.separators+1
	if r.template.hash {
		if captures.HashPresent {
			size += captures.Hash.End - captures.Hash.Start
			levels += strings.Count(name[captures.Hash.Start:captures.Hash.End], "/")
		} else if r.template.literals != 0 {
			size-- // $# occupies the last level, including its preceding slash
			levels--
		}
	}
	if size > r.template.limits.MaxLength || levels > r.template.limits.MaxLevels {
		return 0, false
	}
	for i, count := range r.template.uses {
		span := captures.Plus[i]
		n := span.End - span.Start
		if count != 0 && n > (r.template.limits.MaxLength-size)/count {
			return 0, false
		}
		size += n * count
	}
	return size, size != 0
}

// CaptureCount is the scratch size needed by TopicName. The caller reuses it
// across publications; captures borrow the topic only during substitution.
func (r *BridgeRule) CaptureCount() int { return r.captures }

// BuildsTopic reports whether substitution allocates a topic name. Literal
// templates reuse configuration storage, and both rules keep the source name.
func (r *BridgeRule) BuildsTopic() bool {
	return r.template != nil && (len(r.template.parts) != 1 || r.template.parts[0].capture != 0)
}

// TopicName matches and substitutes a validated rule in one filter traversal.
// The result still needs the ordinary topic policy checks after substitution.
func (r *BridgeRule) TopicName(name string, captures *topic.Captures) (string, bool) {
	mapped, matched := r.MapTopic(name, captures)
	if matched && r.Direction == Both {
		return name, true
	}
	return mapped, matched
}

// MapTopic matches and builds only a replacement name. A matching both rule
// returns an empty replacement, letting readers keep the received topic bytes
// without copying them into a persistent string.
func (r *BridgeRule) MapTopic(name string, captures *topic.Captures) (string, bool) {
	for _, filter := range r.Filters {
		if !topic.MatchCaptures(filter, name, captures) {
			continue
		}
		if r.Direction == Both {
			return "", true
		}
		if !r.BuildsTopic() {
			return r.Topic, true // immutable configuration, with no per-record copy
		}
		size, valid := r.topicSize(name, captures)
		if !valid {
			return "", true // matched, but the substituted result is invalid
		}
		var result strings.Builder
		result.Grow(size)
		for _, part := range r.template.parts {
			text := part.text
			if part.capture == -1 && !captures.HashPresent {
				text = strings.TrimSuffix(text, "/")
			}
			result.WriteString(text)
			switch {
			case part.capture > 0:
				span := captures.Plus[part.capture-1]
				result.WriteString(name[span.Start:span.End])
			case part.capture == -1:
				span := captures.Hash
				result.WriteString(name[span.Start:span.End])
			}
		}
		return result.String(), true
	}
	return "", false
}
