// Package routes names the operations interface for HTTP and operator scopes.
package routes

import "strings"

// The implemented routes' indices in Paths also select their HTTP handlers.
const (
	Health = iota
	Metrics
	ACL
	Config
	Bridges
	Consumers
	Sessions
	Users
	Queues
	Refused
	PositionLost
)

// Paths names every operations route, in metric registration order. Queues
// also names the family of paths with a channel suffix. Treat it as immutable.
var Paths = [...]string{"/health", "/metrics", "/v1/operations/acl", "/v1/operations/config",
	"/v1/operations/bridges", "/v1/operations/consumers", "/v1/operations/sessions", "/v1/operations/users",
	"/v1/operations/queues", "/v1/operations/refused", "/v1/operations/position-lost"}

// Name returns a route's metric label; the final index counts other requests.
func Name(i int) string {
	if i == len(Paths) {
		return "other"
	}
	return strings.TrimPrefix(Paths[i], "/v1/operations/")
}

// Index finds a path's metric label, including a queue's channel suffix.
func Index(path string) int {
	for i, route := range Paths {
		if path == route || (Name(i) == "queues" && strings.HasPrefix(path, route+"/")) {
			return i
		}
	}
	return len(Paths)
}
