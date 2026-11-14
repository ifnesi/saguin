package metrics

import (
	"math"
	"strings"

	"github.com/ifnesi/saguin/internal/packet"
)

// reasonLabels caches metrics.md's spelling of the packet table's names.
// It is indexed by packet type and code, built once and then only read, so
// a per-packet lookup does no hashing or string conversion and allocates
// nothing. No codes or packet sets live here.
var reasonLabels = makeReasonLabels()

func makeReasonLabels() [packet.AUTH + 1][math.MaxUint8 + 1]string {
	var labels [packet.AUTH + 1][math.MaxUint8 + 1]string
	underscores := strings.NewReplacer(" ", "_", "-", "_")
	for typ := packet.CONNECT; typ <= packet.AUTH; typ++ {
		for code := range math.MaxUint8 + 1 {
			name := packet.ReasonName(typ, byte(code))
			if name == "" {
				continue
			}
			labels[typ][code] = strings.ToLower(underscores.Replace(name))
		}
	}
	return labels
}

// ReasonLabel returns the MQTT 5 reason name in lower case, with each space
// or hyphen replaced by an underscore, as metrics.md requires. It returns
// an empty string when the packet cannot carry code.
func ReasonLabel(t packet.Type, code byte) string {
	if int(t) >= len(reasonLabels) {
		return ""
	}
	return reasonLabels[t][code]
}
