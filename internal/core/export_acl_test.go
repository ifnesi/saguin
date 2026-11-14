package core

import "github.com/ifnesi/saguin/internal/connection"

// The seam of the whole-filter guard (acl_delivery_test.go, item 0018e).
// design.md, Testing: "A test inside the code checks only what neither can
// see. These checks cover [...] the busy path"; "The busy path [...] does
// no needless work". The production code defines, unexported:
//
//	// useACL gives c the acl_file data holds, read as at start and given
//	// to the core as main gives it the one it reads (item 0018c): every
//	// CONNECT after it is held to it, its rules those of the CONNECT's
//	// user name.
//	func (c *Core) useACL(data []byte) error
//
// and keeps each connection's compiled rules as the access of its
// connectionState, the rules the core checks a delivery against.

// UseACL is useACL, for the tests outside the package.
func (c *Core) UseACL(data []byte) error { return c.useACL(data) }

// RevokeAccess replaces the compiled rules of conn's connection with those
// of a user matched by no entry of the acl_file UseACL gave, which grant
// nothing (design.md, CONNECT: "A user matched by no users: pattern is
// accepted with no rights"). Its subscriptions keep what SUBSCRIBE worked
// out for them: only a delivery check made after it sees the change.
func (c *Core) RevokeAccess(conn *connection.Conn) {
	c.connections[conn].access = c.access.Compile(nil, "", "")
}
