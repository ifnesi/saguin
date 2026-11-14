package views

// HistoryLimit bounds each core-owned history. Every retained row is returned.
const HistoryLimit = 1000

// HistoryCounts includes events outside the retained rows, including eviction.
type HistoryCounts struct {
	Counts
	Tracked int    `json:"tracked"`
	Beyond  uint64 `json:"beyond"`
}

// RefusedClient describes the latest counted refusal or closure for a client.
type RefusedClient struct {
	ClientID string  `json:"client_id"`
	User     string  `json:"user"`
	Reason   string  `json:"reason"`
	Count    uint64  `json:"count"`
	LastSeen *string `json:"last_seen"`
}

// Refused returns every kept client, most frequently refused first.
type Refused struct {
	HistoryCounts
	Clients []RefusedClient `json:"clients"`
}

// LostReader describes retention observations for one reader and channel.
// RecordsMissed stays null once any represented observation was uncounted.
type LostReader struct {
	Reader        string  `json:"reader"`
	Channel       string  `json:"channel"`
	Kind          string  `json:"kind"`
	Count         uint64  `json:"count"`
	RecordsMissed *uint64 `json:"records_missed"`
	LastPosition  int64   `json:"last_position"`
	LastFloor     int64   `json:"last_floor"`
	LastSeen      *string `json:"last_seen"`
}

// PositionLost returns counted readers first, then those with unknown losses.
type PositionLost struct {
	HistoryCounts
	Readers []LostReader `json:"readers"`
}
