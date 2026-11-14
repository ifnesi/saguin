package testkit

import "strings"

// LogLines returns the lines of log, a broker's standard error, written at
// level ("ERROR"). design.md, Flows, Logging: "Each log line is written on
// standard error. It contains the time the event happened [...] the level
// (DEBUG, INFO, WARN or ERROR), the broker id and the message. These fields
// are separated by single spaces".
func LogLines(log, level string) []string {
	var at []string
	for line := range strings.Lines(log) {
		if f := strings.SplitN(line, " ", 3); len(f) == 3 && f[1] == level {
			at = append(at, strings.TrimSuffix(line, "\n"))
		}
	}
	return at
}

// LinesNaming returns the lines LogLines returns for level whose message
// names any of names, so a test counts the lines of the error it caused
// and no other. design.md, Flows, Logging: the core logs its first fatal
// store error "as one ERROR line naming the error".
func LinesNaming(log, level string, names ...string) []string {
	var naming []string
	for _, line := range LogLines(log, level) {
		for _, name := range names {
			if strings.Contains(line, name) {
				naming = append(naming, line)
				break
			}
		}
	}
	return naming
}
