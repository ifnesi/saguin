package config

// resolve fills inheritance after all broker keys have been read, including
// when channels precede broker in the file. Explicit none overrides defaults.
func (r *channelReader) resolve(seen map[string]bool) error {
	storage := &r.config.Broker.Storage
	filters := make(map[string]string)
	for i := range r.config.Channels {
		c := &r.config.Channels[i]
		key := r.keys[i]
		if !seen[key+".retention_period"] {
			c.RetentionPeriod = storage.DefaultRetentionPeriod
		}
		if c.Type != Latest && !seen[key+".retention_bytes"] {
			c.RetentionBytes = storage.DefaultRetentionBytes
		}
		if c.Type == Queue {
			if !seen[key+".dlq_retention_period"] {
				c.DLQRetentionPeriod = storage.DefaultRetentionPeriod
			}
			if !seen[key+".dlq_retention_bytes"] {
				c.DLQRetentionBytes = storage.DefaultRetentionBytes
			}
			if c.RetentionPeriod != 0 && c.VisibilityTimeout >= c.RetentionPeriod {
				return inFile(c.ConfiguredIn, refuse(key+".visibility_timeout", "must be shorter than retention_period"))
			}
		}
		expanded, err := expandChannelFilter(c.Filter, c.Type == Queue, &r.config.Broker.Limits)
		if err != nil {
			return inFile(c.ConfiguredIn, refuse(key+".filter", err.Error()))
		}
		c.Filters = expanded
		for _, filter := range expanded {
			if earlier, ok := filters[filter]; ok {
				return inFile(c.ConfiguredIn, refuse(key+".filter", "same filter as "+earlier))
			}
			filters[filter] = key + ".filter"
		}
	}
	return nil
}
