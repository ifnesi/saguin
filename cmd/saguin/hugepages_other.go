//go:build !linux

package main

// withoutHugePages returns b: transparent huge pages are Linux's.
func withoutHugePages(b []byte) []byte { return b }
