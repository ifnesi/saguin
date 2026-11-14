package connection

import (
	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/topic"
)

// bridgeTopic confines persistent topic ownership to the bridge role. The
// ordinary client's topic conversion remains temporary validation scratch.
func (r *reader) bridgeTopic(name string, limits topic.Limits) (string, *config.BridgeRule, error) {
	for i := range r.bridgeRules {
		rule := &r.bridgeRules[i]
		if rule.Direction == config.Out {
			continue
		}
		if mapped, ok := rule.MapTopic(name, &r.captures); ok {
			checked := mapped
			if rule.Direction == config.Both {
				checked = name
			}
			return mapped, rule, topic.CheckName(checked, limits)
		}
	}
	return "", nil, topic.ErrOverLimit
}
