package config

import "time"

// ChannelType selects how a configured channel retains and delivers records.
type ChannelType uint8

// Supported channel types.
const (
	Append ChannelType = iota
	Latest
	Queue
)

// Start selects a new reader's initial position.
type Start uint8

// Supported initial reader positions.
const (
	Floor Start = iota
	Tail
)

// RetryMode selects how a queue's retry wait grows with failed attempts.
type RetryMode uint8

// Supported retry modes.
const (
	Fixed RetryMode = iota
	Linear
	Exponential
)

// Retry configures a queue's attempts and the wait between them.
type Retry struct {
	MaxAttempts int
	Mode        RetryMode
	Delay       time.Duration
}

// Channel holds one resolved channel in file order. Filter preserves the
// configured spelling; Filters contains its ordered alternative expansions.
// Fields belonging to another channel type remain zero. ConfiguredIn is the
// defining file's supplied path, or empty for a channel read directly by Parse.
type Channel struct {
	Name                     string
	Type                     ChannelType
	Filter                   string
	Filters                  []string
	Start                    Start
	RetentionPeriod          time.Duration
	RetentionBytes           Size
	MaxBytes                 Size
	TombstoneRetentionPeriod time.Duration
	VisibilityTimeout        time.Duration
	Retry                    Retry
	DLQRetentionPeriod       time.Duration
	DLQRetentionBytes        Size
	ConfiguredIn             string
}

func defaultChannel(name string, kind ChannelType) Channel {
	c := Channel{Name: name, Type: kind, Filter: name + "/#"}
	switch kind {
	case Latest:
		c.TombstoneRetentionPeriod = day
	case Queue:
		c.VisibilityTimeout = 30 * time.Second
		c.Retry = Retry{MaxAttempts: 5, Mode: Fixed, Delay: 5 * time.Second}
	}
	return c
}
