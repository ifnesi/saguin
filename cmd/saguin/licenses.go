package main

import (
	_ "embed"
	"fmt"
	"io"

	brokerlicense "github.com/ifnesi/saguin"
)

//go:embed THIRD-PARTY-NOTICES.md
var thirdPartyNotices string

func licensesCommand(stdout, stderr io.Writer) int {
	if _, err := io.WriteString(stdout, brokerlicense.License+"\n"+thirdPartyNotices); err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
