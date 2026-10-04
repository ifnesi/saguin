package store

// The snapshot decoders fuzzed behind a valid header and checksum, so the
// mutator reaches the body decoder rather than the CRC: nothing panics, and
// whatever decodes re-encodes to something that decodes to the same value
// (invariant 14's "never a plausible different file"). DecodeSnapshot also
// has an every-byte flip and every-prefix truncation sweep; these reach the
// broadcast and sessions files the same way.

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"reflect"
	"testing"
)

func sealedBody(header, body []byte) []byte {
	b := append(append([]byte{}, header[:headerSize]...), body...)
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, crcTable))
}

func bodyOf(file []byte) []byte { return file[headerSize : len(file)-trailerSize] }

func FuzzDecodeSnapshotBodyIsConsistent(f *testing.F) {
	var buf bytes.Buffer
	if err := sample().Encode(&buf); err != nil {
		f.Fatal(err)
	}
	good := buf.Bytes()
	// **The good seed must decode as the fuzzer rebuilds it**, or every
	// input fails to decode, the target returns early on each, and it passes
	// having compared nothing - which is what an encoder that stopped
	// agreeing with its decoder would look like here.
	if _, err := DecodeSnapshot(sealedBody(good, bodyOf(good))); err != nil {
		f.Fatalf("the good seed is refused, so no input below would be compared: %v", err)
	}
	f.Add(bodyOf(good))
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, body []byte) {
		s1, err := DecodeSnapshot(sealedBody(good, body))
		if err != nil {
			return
		}
		var e1 bytes.Buffer
		if err := s1.Encode(&e1); err != nil {
			t.Fatalf("decoded but cannot encode: %v", err)
		}
		s2, err := DecodeSnapshot(e1.Bytes())
		if err != nil {
			t.Fatalf("its own encoding is refused: %v", err)
		}
		var e2 bytes.Buffer
		if err := s2.Encode(&e2); err != nil {
			t.Fatalf("second encode: %v", err)
		}
		s3, err := DecodeSnapshot(e2.Bytes())
		if err != nil {
			t.Fatalf("second encoding refused: %v", err)
		}
		if !reflect.DeepEqual(s2, s3) {
			t.Fatalf("decode∘encode is not stable:\n%+v\n%+v", s2, s3)
		}
	})
}

func FuzzDecodeBroadcastBodyIsConsistent(f *testing.F) {
	var buf bytes.Buffer
	log := &BroadcastSnapshot{Next: 3, Floor: 2,
		Records: []Record{{Offset: 2, MessageID: "m-2", Topic: "state/a", Payload: []byte("on")}}}
	if err := log.Encode(&buf); err != nil {
		f.Fatal(err)
	}
	good := buf.Bytes()
	// **The good seed must decode as the fuzzer rebuilds it**, or every
	// input fails to decode, the target returns early on each, and it passes
	// having compared nothing - which is what an encoder that stopped
	// agreeing with its decoder would look like here.
	if _, err := DecodeBroadcast(sealedBody(good, bodyOf(good))); err != nil {
		f.Fatalf("the good seed is refused, so no input below would be compared: %v", err)
	}
	f.Add(bodyOf(good))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, body []byte) {
		s1, err := DecodeBroadcast(sealedBody(good, body))
		if err != nil {
			return
		}
		var e1 bytes.Buffer
		if err := s1.Encode(&e1); err != nil {
			t.Fatalf("decoded but cannot encode: %v", err)
		}
		s2, err := DecodeBroadcast(e1.Bytes())
		if err != nil {
			t.Fatalf("its own encoding is refused: %v", err)
		}
		var e2 bytes.Buffer
		if err := s2.Encode(&e2); err != nil {
			t.Fatalf("second encode: %v", err)
		}
		s3, err := DecodeBroadcast(e2.Bytes())
		if err != nil {
			t.Fatalf("second encoding refused: %v", err)
		}
		if !reflect.DeepEqual(s2, s3) {
			t.Fatalf("decode∘encode is not stable:\n%+v\n%+v", s2, s3)
		}
	})
}

func FuzzDecodeSessionsBodyIsConsistent(f *testing.F) {
	var buf bytes.Buffer
	s := &SessionsSnapshot{
		Sessions: []SessionState{{
			Session: Session{Client: "a", ExpiryInterval: 60,
				Subscriptions: []SessionSubscription{{Filter: "t/#", QoS: 1}},
				Will:          &SessionWill{Topic: "w", Payload: []byte("bye"), QoS: 1}},
			Window:   5,
			InFlight: []InFlight{{Offset: 1, PacketID: 1, QoS: 1, State: MessageSent}},
		}},
		Groups:   map[string]uint64{"$share/g/t": 1},
		Returned: map[string][]uint64{"$share/g/t": {1}},
	}
	if err := s.Encode(&buf); err != nil {
		f.Fatal(err)
	}
	good := buf.Bytes()
	// **The good seed must decode as the fuzzer rebuilds it**, or every
	// input fails to decode, the target returns early on each, and it passes
	// having compared nothing - which is what an encoder that stopped
	// agreeing with its decoder would look like here.
	if _, err := DecodeSessions(sealedBody(good, bodyOf(good))); err != nil {
		f.Fatalf("the good seed is refused, so no input below would be compared: %v", err)
	}
	f.Add(bodyOf(good))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, body []byte) {
		s1, err := DecodeSessions(sealedBody(good, body))
		if err != nil {
			return
		}
		var e1 bytes.Buffer
		if err := s1.Encode(&e1); err != nil {
			t.Fatalf("decoded but cannot encode: %v", err)
		}
		s2, err := DecodeSessions(e1.Bytes())
		if err != nil {
			t.Fatalf("its own encoding is refused: %v", err)
		}
		var e2 bytes.Buffer
		if err := s2.Encode(&e2); err != nil {
			t.Fatalf("second encode: %v", err)
		}
		s3, err := DecodeSessions(e2.Bytes())
		if err != nil {
			t.Fatalf("second encoding refused: %v", err)
		}
		if !reflect.DeepEqual(s2, s3) {
			t.Fatalf("decode∘encode is not stable:\n%+v\n%+v", s2, s3)
		}
	})
}
