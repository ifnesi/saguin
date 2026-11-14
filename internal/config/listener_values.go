package config

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

// A label has 1 to 63 letters, digits, '-' or '_', with no edge '-'.
// The first alternative captures all-digit labels to reject a numeric last
// label in a host name; literal IP addresses are parsed separately by netip.
var hostLabel = regexp.MustCompile(`^(?:([0-9]{1,63})|[A-Za-z0-9_](?:[A-Za-z0-9_-]{0,61}[A-Za-z0-9_])?)$`)

var (
	listenerPort = regexp.MustCompile(`^\d+$`)
	originPort   = regexp.MustCompile(`^[1-9]\d*$`)
)

// splitAddress validates the written host:port without resolving a name or
// opening a socket. Ports must be decimal numbers, including zero.
func splitAddress(value string) (host string, portNumber uint16, err error) {
	host, port, err := net.SplitHostPort(value)
	if err != nil {
		return "", 0, fmt.Errorf("expected host:port, IPv6 in brackets: %w", err)
	}
	if !validListenHost(host, strings.HasPrefix(value, "[")) {
		return "", 0, fmt.Errorf("invalid listener host %q", host)
	}
	if !listenerPort.MatchString(port) {
		return "", 0, errors.New("expected a decimal port")
	}
	n, err := strconv.ParseUint(port, decimalBase, portBits)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port: %w", err)
	}
	return host, uint16(n), nil
}

// validListenHost accepts every-address hosts and host names, or literal IPs
// of the written family. A zone belongs only to link-local IPv6 unicast.
func validListenHost(host string, bracketed bool) bool {
	if host == "" {
		return !bracketed
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		if bracketed != addr.Is6() || addr.Unmap().IsMulticast() {
			return false
		}
		return addr.Zone() == "" || (addr.Is6() && addr.IsLinkLocalUnicast())
	}
	return !bracketed && validHostName(host)
}

// canonicalHost compares literal IP addresses by value, including IPv4
// mapped IPv6, and leaves DNS names as written. Every unspecified IP and
// the empty host is a wildcard for the configured port-clash rule. Only
// literal IPs prove a loopback address without consulting DNS.
func canonicalHost(host string) (name string, wildcard, loopback bool) {
	if host == "" {
		return "", true, false
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		addr = addr.Unmap()
		return addr.String(), addr.IsUnspecified(), addr.IsLoopback()
	}
	return host, false, false
}

func readOrigins(node *yaml.Node, dst *[]string, key string) error {
	if isNone(node) {
		*dst = nil
		return nil
	}
	if node.Kind != yaml.SequenceNode || node.Tag != "!!seq" {
		return refuse(key, "expected a list of origins or none")
	}
	var origins []string
	if len(node.Content) != 0 {
		origins = make([]string, len(node.Content))
	}
	for i, entry := range node.Content {
		entry = valueNode(entry)
		if entry.Kind != yaml.ScalarNode || entry.Tag != "!!str" {
			return refuse(indexPath(key, i), "expected an origin")
		}
		if _, err := ParseOrigin(entry.Value); err != nil {
			return refuse(indexPath(key, i), err.Error())
		}
		origins[i] = entry.Value
	}
	*dst = origins
	return nil
}

// ParseOrigin validates the same origin syntax in configuration and incoming
// WebSocket requests, so the two cannot disagree about a malformed origin.
func ParseOrigin(value string) (*url.URL, error) {
	u, err := url.Parse(value)
	if err != nil {
		return nil, fmt.Errorf("invalid origin: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" ||
		u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || strings.Contains(value, "#") || u.Opaque != "" {
		return nil, errors.New("expected http:// or https:// with a host and optional port only")
	}
	if strings.HasSuffix(u.Host, ":") {
		return nil, errors.New("expected a decimal port")
	}
	host := strings.TrimSuffix(u.Host, ":"+u.Port())
	if !validOriginHost(host) {
		return nil, errors.New("expected an IPv4 address, bracketed IPv6 address or host name with valid labels")
	}
	if u.Port() != "" {
		if !originPort.MatchString(u.Port()) {
			return nil, errors.New("expected a port from 1 to 65535 without a leading zero")
		}
		if _, err := strconv.ParseUint(u.Port(), decimalBase, portBits); err != nil {
			return nil, fmt.Errorf("invalid origin port: %w", err)
		}
	}
	return u, nil
}

// validOriginHost checks IP literals in their written form. Origins have no
// zones; host names follow the same label rule as listener host names.
func validOriginHost(host string) bool {
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		addr, err := netip.ParseAddr(host[1 : len(host)-1])
		return err == nil && addr.Is6() && addr.Zone() == ""
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		return addr.Is4()
	}
	return validHostName(host)
}

func validHostName(host string) bool {
	var last []string
	for label := range strings.SplitSeq(host, ".") {
		last = hostLabel.FindStringSubmatch(label)
		if last == nil {
			return false
		}
	}
	return len(last) > 1 && last[1] == ""
}
