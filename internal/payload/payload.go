// Package payload prepares message bodies for storage without changing their wire form.
package payload

import (
	"fmt"

	"github.com/klauspost/compress/s2"
)

const pageSize = 4 << 10

// BodyMemory includes the largest S2 destination while a body is being read.
func BodyMemory(n int) int64 {
	bytes := int64(n)
	if n > pageSize {
		bytes += int64(s2.MaxEncodedLen(n))
	}
	return bytes
}

// Compress returns S2 scratch only when storage would be shorter. Its caller
// releases the original body and detaches any borrowed metadata before Compact
// replaces S2's maximum-size allocation with the copy kept through storage.
func Compress(src []byte) []byte {
	if len(src) <= pageSize {
		return nil
	}
	encoded := s2.Encode(nil, src)
	if len(encoded) >= len(src) {
		return nil
	}
	return encoded
}

// Compact retains exactly the encoded length, without S2's unused capacity.
func Compact(encoded []byte) []byte {
	stored := make([]byte, len(encoded))
	copy(stored, encoded)
	return stored
}

// Stored selects the reader's prepared copy, keeping empty payloads non-null.
func Stored(original, compressed []byte) []byte {
	if compressed != nil {
		return compressed
	}
	if original == nil {
		return []byte{}
	}
	return original
}

// Decode checks the stored size before allocating the original body. Damage
// is a store error; an uncompressed payload needs no allocation or copy.
func Decode(stored []byte, size int) ([]byte, error) {
	if len(stored) == size {
		return stored, nil
	}
	if len(stored) > size || size <= pageSize {
		return nil, fmt.Errorf("stored payload length %d differs from payload_size %d", len(stored), size)
	}
	n, err := s2.DecodedLen(stored)
	if err != nil {
		return nil, fmt.Errorf("stored S2 payload: %w", err)
	}
	if n != size {
		return nil, fmt.Errorf("stored S2 payload size %d differs from payload_size %d", n, size)
	}
	decoded, err := s2.Decode(nil, stored)
	if err != nil {
		return nil, fmt.Errorf("stored S2 payload: %w", err)
	}
	return decoded, nil
}
