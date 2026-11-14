package config

import (
	"crypto/tls"
	"fmt"
	"io/fs"
	"math"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/ifnesi/saguin/internal/logger"
	"github.com/ifnesi/saguin/internal/packet"
)

// broker.id and listener names share the same character and length rule.
var identifier = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// A socket mode contains only permission bits, with owner read and write set.
var socketMode = regexp.MustCompile(`^0*[67][0-7]{2}$`)

type unit struct {
	suffix string
	scale  int64
}

// Units are largest first so output uses the largest whole unit.
var (
	sizeUnits    = []unit{{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"", 1}}
	timeUnits    = []unit{{"d", int64(day)}, {"h", int64(time.Hour)}, {"m", int64(time.Minute)}, {"s", int64(time.Second)}, {"ms", int64(time.Millisecond)}}
	logLevels    = []string{"debug", "info", "warn", "error"}
	storeModes   = []string{"full", "normal", "memory"}
	channelTypes = []string{"append", "latest", "queue"}
	starts       = []string{"floor", "tail"}
	retryModes   = []string{"fixed", "linear", "exponential"}
	directions   = []string{"in", "out", "both"}
)

// String returns the configured name of a supported storage mode.
func (m StorageMode) String() string { return storeModes[m] }

// quantity parses exactly decimal digits followed by one allowed unit.
// Checking before multiplication prevents large values from wrapping.
func quantity(value string, units []unit) (int64, error) {
	end := strings.IndexFunc(value, func(r rune) bool { return r < '0' || r > '9' })
	if end < 0 {
		end = len(value)
	}
	n, err := strconv.ParseInt(value[:end], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("expected a whole decimal number: %w", err)
	}
	for _, u := range units {
		if value[end:] == u.suffix {
			if n > math.MaxInt64/u.scale {
				return 0, fmt.Errorf("value %q is too large", value)
			}
			return n * u.scale, nil
		}
	}
	return 0, fmt.Errorf("unsupported unit in %q", value)
}

// ParseSize parses whole decimal bytes, optionally followed by KiB, MiB or
// GiB. It rejects overflow; callers apply their own allowed range.
func ParseSize(value string) (int64, error) {
	return quantity(value, sizeUnits)
}

func (f *field) read(node *yaml.Node, key string, seen map[string]bool) error {
	node = valueNode(node)
	switch dst := f.value.(type) {
	case *[]Bridge:
		return readBridges(node, dst, key, seen)
	case *[]BridgeRule:
		return readBridgeRules(node, dst, key, seen)
	case **BridgeTLS:
		return readBridgeTLS(node, dst, key, seen)
	case *channelReader:
		return dst.read(node, key, seen)
	case *Listen:
		*dst = Listen{}
		if err := readMapping(node, listenSchema(dst, key == "broker.operations.listen"), key, seen); err != nil {
			return err
		}
		if len(dst.TCP)+len(dst.WS)+len(dst.Unix) == 0 {
			return refuse(key, "expected at least one listener kind")
		}
		return nil
	case **Operations:
		*dst = defaultOperations()
		fields := operationsSchema(*dst)
		if err := readMapping(node, fields, key, seen); err != nil {
			return err
		}
		return required(fields, key, seen)
	case *[]Listener:
		return readListeners(node, dst, f.name, key, seen)
	case **TLS:
		return readListenerTLS(node, dst, key, seen)
	case *[]string:
		return readOrigins(node, dst, key)
	}
	if node.Kind != yaml.ScalarNode {
		return refuse(key, "expected a scalar value")
	}
	switch node.Tag {
	case "!!str", "!!int", "!!bool", "!!float":
		// Read the original spelling, rather than YAML's numeric coercions.
	default:
		return refuse(key, "expected a value in the key's documented form")
	}
	value := node.Value
	if value == "none" && f.none {
		// These fields start at zero except for defaults such as max_connect_rate.
		switch dst := f.value.(type) {
		case *string:
			*dst = ""
		case *int:
			*dst = 0
		case *Size:
			*dst = 0
		case *time.Duration:
			*dst = 0
		}
		return nil
	}
	switch dst := f.value.(type) {
	case *string:
		if err := readString(f.name, value, key); err != nil {
			return err
		}
		*dst = value
	case *bool:
		switch value {
		case "true":
			*dst = true
		case "false":
			*dst = false
		default:
			return refuse(key, "expected true or false")
		}
	case *packet.Version:
		switch value {
		case "3.1.1":
			*dst = packet.V311
		case "5":
			*dst = packet.V5
		default:
			return refuse(key, "expected 3.1.1 or 5")
		}
	case *uint16:
		switch value {
		case "1.2":
			*dst = tls.VersionTLS12
		case "1.3":
			*dst = tls.VersionTLS13
		default:
			return refuse(key, "expected 1.2 or 1.3")
		}
	case *fs.FileMode:
		if !socketMode.MatchString(value) {
			return refuse(key, "expected octal permissions at most 0777 with owner read and write set")
		}
		mode, err := strconv.ParseUint(value, octalBase, modeBits)
		if err != nil {
			return refuse(key, "expected octal permission bits: "+err.Error())
		}
		*dst = fs.FileMode(mode)
	case *logger.Level:
		for i, name := range logLevels {
			if value == name {
				*dst = logger.Level(i)
				return nil
			}
		}
		return refuse(key, "expected debug, info, warn or error")
	case *StorageMode:
		for i, name := range storeModes {
			if value == name {
				*dst = StorageMode(i)
				return nil
			}
		}
		return refuse(key, "expected full, normal or memory")
	case *ChannelType:
		n, err := readChoice(value, key, channelTypes)
		*dst = ChannelType(n)
		return err
	case *Start:
		n, err := readChoice(value, key, starts)
		*dst = Start(n)
		return err
	case *RetryMode:
		n, err := readChoice(value, key, retryModes)
		*dst = RetryMode(n)
		return err
	case *Direction:
		n, err := readChoice(value, key, directions)
		*dst = Direction(n)
		return err
	default:
		return f.readNumber(value, key)
	}
	return nil
}

func readString(name, value, key string) error {
	switch name {
	case "peer", "client_id", "username", "topic":
		if value == "" || strings.ContainsRune(value, '\x00') {
			return refuse(key, "expected a nonempty value without U+0000")
		}
	case "filter":
		if value == "" || value == "none" {
			return refuse(key, "expected a topic filter, never none")
		}
	case "id", "name":
		if !identifier.MatchString(value) {
			return refuse(key, "expected 1 to 64 letters, digits, '-' or '_'")
		}
	case "address":
		if _, _, err := splitAddress(value); err != nil {
			return refuse(key, err.Error())
		}
	case "path":
		if strings.TrimSpace(value) == "" || len(value) > maxUnixPathBytes || strings.ContainsRune(value, '\x00') {
			return refuse(key, "expected a nonempty Unix socket path of at most 107 bytes")
		}
		// Check the written final component before Clean can turn a directory
		// into a path that looks like a socket file.
		_, file := filepath.Split(value)
		if strings.HasPrefix(value, "@") || file == "" || file == "." || file == ".." {
			return refuse(key, "expected a Unix socket file path")
		}
	default:
		if !filepath.IsAbs(value) || strings.ContainsRune(value, '\x00') {
			return refuse(key, "expected an absolute path")
		}
	}
	return nil
}

func readChoice(value, key string, choices []string) (int, error) {
	if n := slices.Index(choices, value); n >= 0 {
		return n, nil
	}
	return 0, refuse(key, "expected one of "+strings.Join(choices, ", "))
}

func (f *field) readNumber(value, key string) error {
	var n int64
	var err error
	switch f.value.(type) {
	case *Size:
		n, err = ParseSize(value)
	case *time.Duration:
		units := timeUnits
		if !f.milliseconds {
			units = timeUnits[:len(timeUnits)-1]
		}
		n, err = quantity(value, units)
	default:
		n, err = quantity(value, sizeUnits[len(sizeUnits)-1:])
	}
	if err != nil {
		return refuse(key, err.Error())
	}
	if n < f.minimum || n > f.maximum {
		return refuse(key, fmt.Sprintf("value %q is outside the allowed range [%d, %d]", value, f.minimum, f.maximum))
	}
	switch dst := f.value.(type) {
	case *byte:
		*dst = byte(n)
	case *int:
		*dst = int(n)
	case *Size:
		*dst = Size(n)
	case *time.Duration:
		*dst = time.Duration(n)
	}
	return nil
}

func formatQuantity(n int64, units []unit) (string, error) {
	if n == 0 {
		return "none", nil
	}
	for _, u := range units {
		if n%u.scale == 0 {
			return strconv.FormatInt(n/u.scale, 10) + u.suffix, nil
		}
	}
	return "", fmt.Errorf("value %d cannot be written in whole units", n)
}

func (f *field) text() (string, error) {
	switch value := f.value.(type) {
	case *string:
		if *value == "" && f.none {
			return "none", nil
		}
		return *value, nil
	case *bool:
		return strconv.FormatBool(*value), nil
	case *packet.Version:
		switch *value {
		case packet.V311:
			return "3.1.1", nil
		case packet.V5:
			return "5", nil
		}
	case *uint16:
		switch *value {
		case tls.VersionTLS12:
			return "1.2", nil
		case tls.VersionTLS13:
			return "1.3", nil
		}
	case *fs.FileMode:
		return fmt.Sprintf("%04o", *value), nil
	case *logger.Level:
		if int(*value) < len(logLevels) {
			return logLevels[*value], nil
		}
	case *StorageMode:
		if int(*value) < len(storeModes) {
			return storeModes[*value], nil
		}
	case *ChannelType:
		if int(*value) < len(channelTypes) {
			return channelTypes[*value], nil
		}
	case *Start:
		if int(*value) < len(starts) {
			return starts[*value], nil
		}
	case *RetryMode:
		if int(*value) < len(retryModes) {
			return retryModes[*value], nil
		}
	case *Direction:
		if int(*value) < len(directions) {
			return directions[*value], nil
		}
	case *byte:
		return strconv.Itoa(int(*value)), nil
	case *int:
		if *value == 0 && f.none {
			return "none", nil
		}
		return strconv.Itoa(*value), nil
	case *Size:
		return formatQuantity(int64(*value), sizeUnits)
	case *time.Duration:
		units := timeUnits
		if !f.milliseconds {
			units = timeUnits[:len(timeUnits)-1]
		}
		return formatQuantity(int64(*value), units)
	}
	return "", fmt.Errorf("invalid value for %s", f.name)
}
