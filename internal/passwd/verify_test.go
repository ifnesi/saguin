package passwd

import (
	"crypto/sha512"
	"testing"
)

// An unknown user name costs one fixed check, whatever the file holds:
// config.md, broker.mqtt, password_file: "an unknown user name costs the
// same check as a `$7$` entry of 1,000 iterations, so keep one scheme and
// iteration count in a file: mixing them can let login timing reveal
// whether a user name exists". A `$7$` entry of 1,000 iterations is the
// one `saguin --passwd add` writes, design.md, Operations: "writes `$7$`
// with 1,000 iterations and a 64-byte salt", its hash PBKDF2-HMAC-SHA512
// "with a 64-byte output" (config.md).
//
// Login timing seen from the wire is too noisy to judge, so this is a test
// inside the code (design.md, Testing: "A test inside the code checks only
// what neither can see"). It observes the work deterministically: it
// records every credential Verify hands to the hash, through the seam
// Verify calls once per check:
//
//	// checkCredential hashes password with key's scheme and parameters
//	// and compares the result in constant time; Verify calls it exactly
//	// once, for a known user and an unknown one alike.
//	var checkCredential = credential.matches
//
// The file's first entry is a `$6$` user, whose check is one cheap SHA-512,
// and a later `$7$` user has 50,000 iterations: an unknown name must do
// neither's work, only one PBKDF2 of 1,000 iterations.
func TestVerifyUnknownUserCostsOneThousandIterations(t *testing.T) {
	data := "alice:$6$" + b64(12, 's') + "$" + b64(sha512.Size, 'h') + "\n" +
		"bob:$7$50000$" + b64(16, 'a') + "$" + b64(sha512.Size, 'h') + "\n"
	file, err := Parse(fileName, []byte(data))
	if err != nil {
		t.Fatalf("Parse refused a well-formed file: %v", err)
	}

	var checked []credential
	original := checkCredential
	t.Cleanup(func() { checkCredential = original })
	checkCredential = func(key credential, password string) bool {
		checked = append(checked, key)
		return original(key, password)
	}

	// The names are unknown to the file: one plain, one empty, one differing
	// from the first entry's only in case.
	for _, user := range []string{"carol", "", "Alice"} {
		checked = nil
		if file.Verify(user, "secret") {
			t.Errorf("Verify(%q) admitted a user the file does not name", user)
		}
		if len(checked) != 1 {
			t.Errorf("Verify(%q) ran %d checks; want exactly one", user, len(checked))
			continue
		}
		key := checked[0]
		if key.count != 1000 || len(key.salt) != 64 || len(key.sum) != sha512.Size {
			scheme := "$7$"
			if key.count == 0 {
				scheme = "$6$"
			}
			t.Errorf("Verify(%q) checked a %s credential of %d iterations, a %d-byte salt and a %d-byte hash; "+
				"want $7$, 1000 iterations, a 64-byte salt and a %d-byte hash, whatever the file's entries use",
				user, scheme, key.count, len(key.salt), len(key.sum), sha512.Size)
		}
	}
}
