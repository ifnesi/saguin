package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/ifnesi/saguin/internal/operations/views"
)

// Statement identifies SQL prepared at startup on its owning connections.
type Statement uint8

// The store's operational statements. AddRecord binds every records column
// in storage.md's order; nullable columns take nil arguments.
const (
	AddChannel Statement = iota
	AddRecord
	advanceChannel
	updateLatestRecord
	updateLatestHeld
	addQueueJob
	moveDeadLetter
	readDeadLetterProperties
	deleteQueueJob
	spendQueueAttempt
	failQueueAttempt
	readQueueRetries
	nextQueueRetry
	readyQueueJob
	countQueueJobs
	readQueueMetadata
	LoadChannels
	channelBelowSaved
	readView
	loadSession
	addSession
	updateSession
	leaveSession
	deleteSession
	deleteSubscriptions
	deleteSubscriptionStarts
	deleteWill
	addWill
	loadWill
	loadWillInfo
	scheduleWill
	dueWills
	retryWills
	restoreWills
	nextWill
	deletePositions
	deleteSessionInflight
	deleteSessionHeld
	dueSessions
	highestSession
	leaveConnectedSessions
	countRestoredSessions
	loadSessionSubscriptions
	addSubscription
	updateSubscription
	addSubscriptionStart
	addSubscriptionPosition
	seekSessionPosition
	readSeekPosition
	readCurrentValueSize
	readCurrentValuePayload
	supersedeSeekDeliveries
	removeSubscription
	removeSubscriptionStarts
	removeSubscriptionPosition
	supersedeSubscriptionDeliveries
	readRecordMetadata
	addSessionDelivery
	releaseSessionDelivery
	settleSessionDelivery
	markSessionDelivery
	pruneSessionMarkers
	deleteSessionMarkers
	moveSessionPosition
	rewindSessionPosition
	restoreSessionDelivery
	supersedeSessionDeliveries
	loadSessionPosition
	loadDeliverySequence
	readSessionDeliveries
	nextSessionRelease
	deleteOrphanDeliveries
	retainedTotals
	retainedCharge
	clearRetainedTopic
	expireRetained
	nextRetainedDeadline
	readRetainedMetadata
	readRetentionRecords
	deleteRetentionRecords
	advanceRetention
	retentionSubscriptions
	retentionHasDelivery
	retentionDeliveries
	readChannelBytes
	readMetricCounts
	countChannelRecords
	readMetricPositions
	dropRecords
	dropPositions
	dropStarts
	dropSessionInflight
	dropHeld
	dropJobs
	dropShareCursors
	dropShareReturned
	dropBridgeCursors
	dropBridgeInflight
	dropChannel
	endingHeld
	addHeldPublish
	readHeldPublish
	loadHeldIdentifiers
	loadBridges
	addBridge
	setBridgeReplay
	addBridgeRule
	loadBridgeRule
	addBridgeCursor
	loadBridgeCursors
	loadBridgeSequence
	loadBridgeIdentifier
	deleteBridgeDeliveries
	readBridgePreparations
	loadBridgeRules
	removeBridgeCursors
	pruneBridgeRules
	pruneBridgeRule
	pruneBridge
	retentionBridgeCursor
	retentionBridgeDelivery
	readBridgeDeliveries
	nextBridgeRelease
	addBridgeDelivery
	releaseBridgeDelivery
	settleBridgeDelivery
	moveBridgeCursor
	rewindBridgeCursor
	moveHeldPublish
	deleteHeldPublish
	heldChannelTotals
	retainedChargesTotal
	dueHeld
	deleteOrphanHeld
	loadSharedGroup
	loadSharedCursors
	readSharedReturned
	addSharedGroup
	selectSharedMember
	saveSharedCursor
	returnSharedRecord
	settleSharedRecord
	endSharedCursors
	endSharedReturned
	endSharedGroup
	highestSharedGroup
	readMetadataDisposition
	loadEndingShares
	returnSessionShares
	countEndingShares
	cleanupSharedGroup
	retentionSharedGroups
	retentionSharedReturned
	retentionSharedDeliveries
	retainSharedReturned
	dueSharedGroups
	orphanSharedGroups
	saveSharedEvidence
	pruneSharedEvidence
	terminalSharedDelivery
	terminalGroupShares
	retireSharedDelivery
	settlingSharedGroup
	terminalSessionShares
	returnVolatileShares
	readVolatileShares
	pruneStoredSharedEvidence
	pruneEndedSharedEvidence
	checkpointLog
	totalChanges
	readOperationsSessions
	readOperationsPartitions
	readOperationsPositions
	readOperationsCursors
	readOperationsJobs
	statementCount
)

// Channel removal uses only row deletions, with the channel row last.
var dropChannelStatements = [...]Statement{dropRecords, dropPositions, dropStarts,
	dropSessionInflight, dropHeld, dropJobs, dropShareCursors,
	dropShareReturned, dropBridgeCursors, dropBridgeInflight, dropChannel}

type statementSpec struct {
	name  string
	sql   string
	write bool // writer-owned; includes lookups inside a retention mutation
}

// willMemorySQL reserves the decoded body and the compressed source that a
// store reader holds together. The same value also fixes LoadWill's source.
const willMemorySQL = `length(CAST(w.topic AS BLOB)) + w.payload_size + coalesce(length(w.properties), 0)
	+ CASE WHEN length(w.payload) < w.payload_size THEN length(w.payload) ELSE 0 END`

// Terminal evidence for protocol-only exchanges must survive cursor pruning
// until those exchanges finish; ordinary group evidence needs no extra owner.
const sharedEvidenceUnownedSQL = `NOT EXISTS (SELECT 1 FROM session_inflight i WHERE i.share > 0
	AND i.share = share_returned.share AND i.channel = share_returned.channel
	AND i."offset" = share_returned."offset" AND i.phase IN (1, 2))`

const sharedExchangeTerminalSQL = `EXISTS (SELECT 1 FROM share_returned b WHERE b.share = i.share
	AND b.channel = i.channel AND b."offset" = i."offset" AND b.state = 3)`

// statementSQL is the single table of SQL used by store operations. Writes
// are prepared only on the writer; reads on every connection that loads or
// serves state. Adding an operation adds its SQL here, not in its write loop.
var statementSQL = [statementCount]statementSpec{
	readOperationsSessions: {name: "ReadOperationsSessions", sql: `SELECT s.id, s.client, s.user, s.protocol, s.expiry,
		s.listener, s.remote, s.keepalive, s.clean_start,
		(SELECT count(*) FROM subscriptions WHERE session = s.id)
		FROM sessions s WHERE s.client > ? ORDER BY s.client LIMIT ` + strconv.Itoa(MetadataPage)},
	readOperationsPartitions: {name: "ReadOperationsPartitions", sql: `SELECT filter, partitions FROM subscriptions
		WHERE session = ? AND partitions <> '' ORDER BY filter`},
	readOperationsPositions: {name: "ReadOperationsPositions", sql: `SELECT p.session, s.client, p."offset", p.last_seen, '', ''
		FROM positions p JOIN sessions s ON s.id = p.session WHERE p.channel = ? AND p.session > ?
		ORDER BY p.session LIMIT ` + strconv.Itoa(MetadataPage)},
	readOperationsCursors: {name: "ReadOperationsCursors", sql: `SELECT r.id, b.name, c."offset", c.last_seen, r.filter, r.topic
		FROM bridge_cursors c JOIN bridge_rules r ON r.id = c.rule JOIN bridges b ON b.id = r.bridge
		WHERE c.channel = ? AND r.id > ? ORDER BY r.id LIMIT ` + strconv.Itoa(MetadataPage)},
	readOperationsJobs: {name: "ReadOperationsJobs", sql: `SELECT j."offset", r.topic, j.spent, j.first_attempt, j.last_attempt, j.next_try
		FROM queue_jobs j JOIN records r ON r.channel = j.channel AND r."offset" = j."offset"
		WHERE j.channel = ? ORDER BY j."offset" LIMIT ` + strconv.Itoa(views.Limit)},
	checkpointLog: {"checkpoint log", `PRAGMA wal_checkpoint(TRUNCATE)`, true},
	totalChanges:  {"total changes", `SELECT total_changes()`, true},
	addQueueJob: {name: "AddQueueJob", write: true, sql: `INSERT INTO queue_jobs
		(channel, "offset", spent, failed) VALUES (?, ?, 0, 0)`},
	moveDeadLetter: {name: "MoveDeadLetter", write: true, sql: `UPDATE records
		SET channel = ?3, "offset" = ?4, time = ?5, topic = ?6, properties = ?7
		WHERE channel = ?1 AND "offset" = ?2
		RETURNING length(CAST(topic AS BLOB)) + payload_size + length(properties)`},
	readDeadLetterProperties: {name: "ReadDeadLetterProperties", write: true,
		sql: `SELECT properties FROM records WHERE channel = ? AND "offset" = ?`},
	spendQueueAttempt: {name: "SpendQueueAttempt", write: true, sql: `UPDATE queue_jobs SET spent = spent + 1,
 first_attempt = coalesce(first_attempt, ?3), last_attempt = ?3 WHERE channel = ?1 AND "offset" = ?2`},
	failQueueAttempt: {name: "FailQueueAttempt", write: true, sql: `UPDATE queue_jobs SET failed = failed + 1, next_try = ?3 WHERE channel = ?1 AND "offset" = ?2`},
	readQueueRetries: {name: "ReadQueueRetries", sql: `SELECT "offset" FROM queue_jobs
 WHERE channel = ?1 AND next_try IS NOT NULL AND next_try <= ?2 ORDER BY next_try, "offset" LIMIT 256`},
	nextQueueRetry: {name: "NextQueueRetry", sql: `SELECT min(next_try) FROM queue_jobs WHERE channel = ?1 AND next_try IS NOT NULL`},
	readyQueueJob:  {name: "ReadyQueueJob", write: true, sql: `UPDATE queue_jobs SET next_try = NULL WHERE channel = ?1 AND "offset" = ?2`},
	countQueueJobs: {name: "CountQueueJobs", sql: `SELECT count(*),
 (SELECT count(*) FROM queue_jobs WHERE channel = ?1 AND next_try IS NOT NULL),
 (SELECT min(next_try) FROM queue_jobs WHERE channel = ?1 AND next_try IS NOT NULL)
 FROM queue_jobs WHERE channel = ?1`},
	readQueueMetadata: {name: "ReadQueueMetadata", sql: recordMetadataFieldsSQL + `, j.spent, j.failed FROM queue_jobs j JOIN records r ON r.channel = j.channel AND r."offset" = j."offset"
 WHERE j.channel = ?1 AND j."offset" >= ?2 AND j."offset" < ?3 AND j.next_try IS NULL ORDER BY j."offset" LIMIT ?4`},
	deleteQueueJob: {name: "DeleteQueueJob", write: true, sql: `DELETE FROM queue_jobs WHERE channel = ? AND "offset" = ?`},
	saveSharedEvidence: {name: "SaveSharedEvidence", write: true, sql: `INSERT INTO share_returned (share, channel, "offset", state, session, qos)
		VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (share, channel, "offset") DO UPDATE SET
		state = excluded.state, session = excluded.session, qos = excluded.qos`},
	pruneSharedEvidence: {name: "PruneSharedEvidence", write: true, sql: `DELETE FROM share_returned
		WHERE share = ? AND channel = ? AND "offset" < ? AND state = 3 AND ` + sharedEvidenceUnownedSQL},
	pruneStoredSharedEvidence: {name: "PruneStoredSharedEvidence", write: true, sql: `DELETE FROM share_returned
		WHERE share = ?1 AND channel = ?2 AND state = 3 AND "offset" <
		(SELECT "offset" FROM share_cursors WHERE share = ?1 AND channel = ?2) AND ` + sharedEvidenceUnownedSQL},
	pruneEndedSharedEvidence: {name: "PruneEndedSharedEvidence", write: true, sql: `DELETE FROM share_returned
		WHERE share = ? AND state = 3 AND EXISTS (SELECT 1 FROM share_cursors c
		WHERE c.share = share_returned.share AND c.channel = share_returned.channel AND c."offset" > share_returned."offset") AND ` + sharedEvidenceUnownedSQL},
	terminalGroupShares: {name: "TerminalGroupShares", write: true, sql: `INSERT INTO share_returned (share, channel, "offset", state, session, qos)
		SELECT share, channel, "offset", 3, NULL, NULL FROM session_inflight WHERE share > 0 AND share = ? AND phase IN (1, 2)
		ON CONFLICT (share, channel, "offset") DO UPDATE SET state = 3, session = NULL, qos = NULL`},
	retireSharedDelivery: {name: "RetireSharedDelivery", write: true, sql: `DELETE FROM share_returned WHERE state = 3
		AND (share, channel, "offset") IN (SELECT share, channel, "offset" FROM session_inflight
		WHERE session = ? AND packet_id = ? AND share > 0 AND phase IN (1, 2))`},
	settlingSharedGroup: {name: "SettlingSharedGroup", write: true, sql: `SELECT g.id, g.share
		FROM session_inflight i JOIN share_groups g ON g.id = i.share
		WHERE i.session = ? AND i.packet_id = ? AND i.share > 0 AND i.phase IN (1, 2)`},
	terminalSharedDelivery: {name: "TerminalSharedDelivery", write: true, sql: `INSERT INTO share_returned (share, channel, "offset", state, session, qos)
		SELECT share, channel, "offset", 3, NULL, NULL FROM session_inflight
		WHERE session = ? AND packet_id = ? AND share > 0 AND phase IN (1, 2)
		AND NOT EXISTS (SELECT 1 FROM share_cursors c WHERE c.share = session_inflight.share AND c.channel = session_inflight.channel AND c."offset" > session_inflight."offset")
		ON CONFLICT (share, channel, "offset") DO UPDATE SET state = 3, session = NULL, qos = NULL`},
	terminalSessionShares: {name: "TerminalSessionShares", write: true, sql: `INSERT INTO share_returned (share, channel, "offset", state, session, qos)
		SELECT share, channel, "offset", 3, NULL, NULL FROM session_inflight WHERE session = ? AND share > 0 AND qos = 2 AND phase IN (1, 2)
		AND NOT EXISTS (SELECT 1 FROM share_cursors c WHERE c.share = session_inflight.share AND c.channel = session_inflight.channel AND c."offset" > session_inflight."offset")
		ON CONFLICT (share, channel, "offset") DO UPDATE SET state = 3, session = NULL, qos = NULL`},
	returnVolatileShares: {name: "ReturnVolatileShares", write: true, sql: `UPDATE share_returned SET
		state = CASE WHEN qos = 1 AND EXISTS (SELECT 1 FROM records r JOIN channels c ON c.id = r.channel
		WHERE r.channel = share_returned.channel AND r."offset" = share_returned."offset" AND r."offset" >= c.floor) THEN 1 ELSE 3 END,
		session = NULL, qos = NULL WHERE session = ? AND state = 2`},
	readVolatileShares: {name: "ReadVolatileShares", write: true, sql: `SELECT share, channel, "offset", qos,
		EXISTS (SELECT 1 FROM records r JOIN channels c ON c.id = r.channel WHERE r.channel = b.channel AND r."offset" = b."offset" AND r."offset" >= c.floor), session, packet_id, terminal
		FROM (SELECT share, channel, "offset", qos, 0 AS session, 0 AS packet_id, 0 AS terminal FROM share_returned
		WHERE state = 2 AND (share, channel, "offset") > (?1, ?2, ?3)
		UNION ALL SELECT share, channel, "offset", qos, session, packet_id, ` + sharedExchangeTerminalSQL + ` FROM session_inflight i
		WHERE share > 0 AND phase IN (1, 2) AND (share, channel, "offset") > (?1, ?2, ?3)
		AND NOT EXISTS (SELECT 1 FROM sessions s WHERE s.id = i.session)) b
		ORDER BY share, channel, "offset" LIMIT 256`},
	dueSharedGroups: {name: "DueSharedGroups", sql: `SELECT id, share, 1, 0, 0, 0, NULL, 0 FROM share_groups g
		WHERE id > ? AND NOT EXISTS (SELECT 1 FROM subscriptions sub JOIN sessions s ON s.id = sub.session WHERE sub.filter = g.share)
		AND NOT EXISTS (SELECT 1 FROM share_returned b WHERE b.share = g.id AND b.state = 2)
		AND NOT EXISTS (SELECT 1 FROM session_inflight i WHERE i.share > 0 AND i.share = g.id AND i.phase IN (1, 2))
		ORDER BY id LIMIT ?`},
	orphanSharedGroups: {name: "OrphanSharedGroups", write: true, sql: `SELECT id FROM share_groups g
		WHERE NOT EXISTS (SELECT 1 FROM subscriptions sub JOIN sessions s ON s.id = sub.session WHERE sub.filter = g.share)
		AND NOT EXISTS (SELECT 1 FROM share_returned b WHERE b.share = g.id AND b.state = 2)
		AND NOT EXISTS (SELECT 1 FROM session_inflight i WHERE i.share > 0 AND i.share = g.id AND i.phase IN (1, 2))
		ORDER BY id LIMIT 256`},
	retentionSharedGroups: {name: "RetentionSharedGroups", write: true, sql: `SELECT g.id, coalesce(c."offset", ?2 + 1), g.share
		FROM share_groups g LEFT JOIN share_cursors c ON c.share = g.id AND c.channel = ?1
		WHERE g.id > ?3 AND (c."offset" <= ?2 OR EXISTS (SELECT 1 FROM share_returned r
		WHERE r.share = g.id AND r.channel = ?1 AND r."offset" BETWEEN ?4 AND ?2)) ORDER BY g.id LIMIT 256`},
	retentionSharedReturned: {name: "RetentionSharedReturned", write: true, sql: `SELECT "offset" FROM share_returned
		WHERE state = 1 AND share = ? AND channel = ? AND "offset" > ? AND "offset" <= ? ORDER BY "offset" LIMIT 256`},
	retentionSharedDeliveries: {name: "RetentionSharedDeliveries", write: true, sql: `SELECT "offset" FROM session_inflight
		WHERE phase IN (1, 2) AND share > 0 AND share = ?1 AND channel = ?2 AND "offset" > ?3 AND "offset" <= ?4
		UNION SELECT "offset" FROM share_returned WHERE state IN (2, 3) AND share = ?1 AND channel = ?2
		AND "offset" > ?3 AND "offset" <= ?4 ORDER BY "offset" LIMIT 256`},
	retainSharedReturned: {name: "RetainSharedReturned", write: true, sql: `DELETE FROM share_returned
		WHERE share = ?1 AND channel = ?2 AND "offset" IN (VALUES (?3)) AND state <> 2`},
	loadEndingShares: {name: "LoadEndingShares", sql: `SELECT g.id, g.share FROM share_groups g
		WHERE g.id > ?2 AND (EXISTS (SELECT 1 FROM subscriptions sub WHERE sub.session = ?1 AND sub.filter = g.share)
		OR EXISTS (SELECT 1 FROM session_inflight i INDEXED BY session_inflight_shared WHERE i.session = ?1 AND i.share > 0 AND i.share = g.id AND i.phase IN (1, 2))
		OR EXISTS (SELECT 1 FROM share_returned r WHERE r.session = ?1 AND r.share = g.id AND r.state = 2)) ORDER BY g.id LIMIT 256`},
	returnSessionShares: {name: "ReturnSessionShares", write: true, sql: `INSERT INTO share_returned (share, channel, "offset", state, session, qos)
		SELECT i.share, i.channel, i."offset", 1, NULL, NULL FROM session_inflight i JOIN channels c ON c.id = i.channel
		JOIN records r ON r.channel = i.channel AND r."offset" = i."offset"
		WHERE i.session = ? AND i.share > 0 AND i.qos = 1 AND i.phase IN (1, 2) AND i."offset" >= c.floor
		AND NOT ` + sharedExchangeTerminalSQL + `
		ON CONFLICT (share, channel, "offset") DO UPDATE SET state = 1, session = NULL, qos = NULL`},
	countEndingShares: {name: "CountEndingShares", write: true, sql: `SELECT channel, count(*) FROM (SELECT channel FROM session_inflight i
		WHERE session = ?1 AND share > 0 AND qos = 2 AND phase IN (1, 2) AND NOT ` + sharedExchangeTerminalSQL + `
		UNION ALL SELECT channel FROM share_returned WHERE session = ?1 AND state = 2 AND qos = 2) GROUP BY channel ORDER BY channel`},
	cleanupSharedGroup: {name: "CleanupSharedGroup", write: true, sql: `SELECT (SELECT count(*) FROM subscriptions sub
		JOIN sessions s ON s.id = sub.session WHERE sub.filter = g.share) FROM share_groups g WHERE g.id = ?`},
	readMetadataDisposition: {name: "ReadMetadataDisposition", sql: `WITH page("offset") AS MATERIALIZED (VALUES (?4)),
		settled AS MATERIALIZED (SELECT DISTINCT "offset" FROM session_inflight WHERE ?2 > 0
		AND session = ?2 AND share = 0 AND channel = ?1 AND phase = 3 AND moves = 1 AND "offset" IN (SELECT "offset" FROM page))
		SELECT p."offset" FROM page p WHERE p."offset" IN (SELECT "offset" FROM settled)
		OR (?3 > 0 AND (EXISTS (SELECT 1 FROM session_inflight i WHERE i.share > 0 AND i.share = ?3
		AND i.channel = ?1 AND i."offset" = p."offset" AND i.phase IN (1, 2))
		OR EXISTS (SELECT 1 FROM share_returned b WHERE b.share = ?3 AND b.channel = ?1
		AND b."offset" = p."offset" AND b.state IN (2, 3)))) ORDER BY p."offset"`},
	loadSharedGroup: {name: "LoadSharedGroup", sql: `SELECT id, last_selected,
		(SELECT count(*) FROM subscriptions sub JOIN sessions s ON s.id = sub.session WHERE sub.filter = g.share)
		FROM share_groups g WHERE share = ?`},
	loadSharedCursors: {name: "LoadSharedCursors", sql: `SELECT channel, "offset" FROM share_cursors WHERE share = ? ORDER BY channel`},
	readSharedReturned: {name: "ReadSharedReturned", sql: `SELECT channel, "offset" FROM share_returned
		WHERE state = 1 AND share = ? AND (channel, "offset") > (?, ?) ORDER BY channel, "offset" LIMIT ?`},
	addSharedGroup:     {name: "AddSharedGroup", write: true, sql: `INSERT INTO share_groups (id, share, last_selected) VALUES (?, ?, NULL)`},
	selectSharedMember: {name: "SelectSharedMember", write: true, sql: `UPDATE share_groups SET last_selected = ? WHERE id = ?`},
	saveSharedCursor: {name: "SaveSharedCursor", write: true, sql: `INSERT INTO share_cursors (share, channel, "offset") VALUES (?, ?, ?)
		ON CONFLICT (share, channel) DO UPDATE SET "offset" = excluded."offset"`},
	returnSharedRecord: {name: "ReturnSharedRecord", write: true, sql: `INSERT INTO share_returned (share, channel, "offset", state, session, qos)
		SELECT ?, ?, ?, 1, NULL, NULL WHERE EXISTS (SELECT 1 FROM channels c JOIN records r ON r.channel = c.id
		WHERE c.id = ?2 AND r."offset" = ?3 AND r."offset" >= c.floor)
		ON CONFLICT (share, channel, "offset") DO UPDATE SET state = 1, session = NULL, qos = NULL`},
	settleSharedRecord: {name: "SettleSharedRecord", write: true, sql: `DELETE FROM share_returned WHERE share = ? AND channel = ? AND "offset" = ?`},
	endSharedCursors:   {name: "EndSharedCursors", write: true, sql: `DELETE FROM share_cursors WHERE share = ?`},
	endSharedReturned:  {name: "EndSharedReturned", write: true, sql: `DELETE FROM share_returned WHERE share = ? AND ` + sharedEvidenceUnownedSQL},
	endSharedGroup: {name: "EndSharedGroup", write: true, sql: `DELETE FROM share_groups WHERE id = ?
		AND NOT EXISTS (SELECT 1 FROM session_inflight WHERE share > 0 AND share = share_groups.id AND phase IN (1, 2))`},
	highestSharedGroup: {name: "HighestSharedGroup", sql: `SELECT max(id) FROM (
		SELECT coalesce(max(id), 0) AS id FROM share_groups UNION ALL
		SELECT coalesce(max(share), 0) FROM session_inflight WHERE share > 0 UNION ALL
		SELECT coalesce(max(share), 0) FROM share_cursors UNION ALL SELECT coalesce(max(share), 0) FROM share_returned)`},
	endingHeld: {name: "EndingHeld", write: true, sql: `SELECT packet_id, channel,
		length(CAST(topic AS BLOB)) + payload_size + coalesce(length(properties), 0), topic,
		channel = 1 AND retain = 1 AND payload_size > 0 FROM held_publishes
		WHERE session = ? AND bridge = ? AND packet_id > ? ORDER BY packet_id LIMIT 256`},
	addHeldPublish: {name: "AddHeldPublish", write: true, sql: `INSERT INTO held_publishes
		(session, bridge, packet_id, channel, id, time, topic, publisher, expires, properties, retain, payload, payload_size)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`},
	readHeldPublish: {name: "ReadHeldPublish", sql: `SELECT channel, time, topic, expires,
		coalesce(length(properties), 0), retain, payload_size,
		(SELECT type = 2 FROM channels WHERE id = channel) FROM held_publishes WHERE session = ? AND bridge = ? AND packet_id = ?`},
	loadHeldIdentifiers: {name: "LoadHeldIdentifiers", sql: `SELECT packet_id, channel FROM held_publishes
		WHERE session = ? AND bridge = ? ORDER BY rowid`},
	loadBridges:             {name: "LoadBridges", sql: `SELECT id, name, replay_owed FROM bridges ORDER BY id`},
	addBridge:               {name: "AddBridge", write: true, sql: `INSERT INTO bridges (id, name, replay_owed) VALUES (?, ?, 0)`},
	setBridgeReplay:         {name: "SetBridgeReplay", write: true, sql: `UPDATE bridges SET replay_owed = ? WHERE id = ?`},
	addBridgeRule:           {name: "AddBridgeRule", write: true, sql: `INSERT INTO bridge_rules (bridge, filter, topic) VALUES (?, ?, ?) ON CONFLICT (bridge, filter, topic) DO NOTHING`},
	loadBridgeRule:          {name: "LoadBridgeRule", write: true, sql: `SELECT id FROM bridge_rules WHERE bridge = ? AND filter = ? AND topic = ?`},
	addBridgeCursor:         {name: "AddBridgeCursor", write: true, sql: `INSERT INTO bridge_cursors (rule, channel, "offset", last_seen) VALUES (?, ?, ?, NULL) ON CONFLICT (rule, channel) DO NOTHING`},
	loadBridgeCursors:       {name: "LoadBridgeCursors", sql: `SELECT r.id, c.channel, c."offset", c.last_seen FROM bridge_rules r JOIN bridge_cursors c ON c.rule = r.id WHERE r.bridge = ? ORDER BY r.id, c.channel`},
	retentionBridgeCursor:   {name: "RetentionBridgeCursor", write: true, sql: `SELECT "offset" FROM bridge_cursors WHERE rule = ? AND channel = ?`},
	retentionBridgeDelivery: {name: "RetentionBridgeDelivery", write: true, sql: `SELECT EXISTS (SELECT 1 FROM bridge_inflight WHERE rule = ? AND channel = ? AND "offset" = ?)`},
	loadBridgeSequence:      {name: "LoadBridgeSequence", sql: `SELECT coalesce(max(seq), 0), count(*) FROM bridge_inflight WHERE bridge = ?`},
	loadBridgeIdentifier:    {name: "LoadBridgeIdentifier", sql: `SELECT packet_id FROM bridge_inflight WHERE bridge = ? ORDER BY packet_id`},
	deleteBridgeDeliveries:  {name: "DeleteBridgeDeliveries", write: true, sql: `DELETE FROM bridge_inflight WHERE bridge = ?`},
	readBridgePreparations: {name: "ReadBridgePreparations", write: true, sql: `SELECT packet_id, seq, channel, "offset", qos, rule,
		EXISTS (SELECT 1 FROM bridge_cursors WHERE rule = i.rule AND channel = i.channel)
		FROM bridge_inflight i WHERE bridge = ? AND seq > ? ORDER BY seq LIMIT 256`},
	loadBridgeRules: {name: "LoadBridgeRules", write: true, sql: `SELECT id, filter, topic,
		EXISTS (SELECT 1 FROM bridge_cursors WHERE rule = r.id) FROM bridge_rules r
		WHERE bridge = ? AND id > ? ORDER BY id LIMIT 256`},
	removeBridgeCursors: {name: "RemoveBridgeCursors", write: true, sql: `DELETE FROM bridge_cursors WHERE rule = ?`},
	pruneBridgeRules: {name: "PruneBridgeRules", write: true, sql: `DELETE FROM bridge_rules WHERE bridge = ?
		AND NOT EXISTS (SELECT 1 FROM bridge_cursors WHERE rule = bridge_rules.id)
		AND NOT EXISTS (SELECT 1 FROM bridge_inflight WHERE rule = bridge_rules.id)`},
	pruneBridgeRule: {name: "PruneBridgeRule", write: true, sql: `DELETE FROM bridge_rules WHERE id = ?
		AND NOT EXISTS (SELECT 1 FROM bridge_cursors WHERE rule = bridge_rules.id)
		AND NOT EXISTS (SELECT 1 FROM bridge_inflight WHERE rule = bridge_rules.id)`},
	pruneBridge: {name: "PruneBridge", write: true, sql: `DELETE FROM bridges WHERE id = ?1
		AND NOT EXISTS (SELECT 1 FROM records WHERE bridge = ?1)
		AND NOT EXISTS (SELECT 1 FROM bridge_rules WHERE bridge = ?1)
		AND NOT EXISTS (SELECT 1 FROM bridge_inflight WHERE bridge = ?1)
		AND NOT EXISTS (SELECT 1 FROM held_publishes WHERE bridge = ?1)`},
	readBridgeDeliveries: {name: "ReadBridgeDeliveries", sql: `SELECT i.packet_id, i.seq, i.channel, i."offset", i.qos, i.phase = 2, coalesce(i."offset" >= b."offset", 0), 0, NULL, NULL, NULL,
		coalesce(r.published_retained, 0) AND i.channel = 1, 0, NULL,
		r."offset" IS NULL OR i."offset" < c.floor, coalesce(r.topic, ''), r.properties, r.expires, coalesce(r.payload_size, 0), CASE WHEN length(r.payload) < r.payload_size THEN length(r.payload) ELSE 0 END,
		i.rule, coalesce(r.id, ''), rules.filter, rules.topic, coalesce(r.qos, i.qos)
		FROM bridge_inflight i JOIN channels c ON c.id = i.channel JOIN bridge_rules rules ON rules.id = i.rule
		LEFT JOIN bridge_cursors b ON b.rule = i.rule AND b.channel = i.channel
		LEFT JOIN records r ON r.channel = i.channel AND r."offset" = i."offset"
		WHERE i.bridge = ? AND i.seq <= ? AND ((i.phase = 1 AND i.seq > ? AND ? = 0) OR (i.phase = 2 AND i.seq > ? AND ? = 0)) ORDER BY i.seq LIMIT ?`},
	nextBridgeRelease:     {name: "NextBridgeRelease", sql: `SELECT coalesce(min(seq), 0) FROM bridge_inflight WHERE bridge = ? AND seq > ? AND seq <= ? AND phase = 2`},
	addBridgeDelivery:     {name: "AddBridgeDelivery", write: true, sql: `INSERT INTO bridge_inflight (bridge, packet_id, seq, channel, "offset", qos, rule, phase) VALUES (?, ?, ?, ?, ?, ?, ?, 1)`},
	releaseBridgeDelivery: {name: "ReleaseBridgeDelivery", write: true, sql: `UPDATE bridge_inflight SET phase = 2 WHERE bridge = ? AND packet_id = ?`},
	settleBridgeDelivery:  {name: "SettleBridgeDelivery", write: true, sql: `DELETE FROM bridge_inflight WHERE bridge = ? AND packet_id = ?`},
	moveBridgeCursor:      {name: "MoveBridgeCursor", write: true, sql: `UPDATE bridge_cursors SET "offset" = ?1, last_seen = ?2 WHERE rule = ?3 AND channel = ?4 AND "offset" < ?5`},
	rewindBridgeCursor:    {name: "RewindBridgeCursor", write: true, sql: `UPDATE bridge_cursors SET "offset" = ?, last_seen = ? WHERE rule = ? AND channel = ?`},
	moveHeldPublish: {name: "MoveHeldPublish", write: true, sql: `INSERT INTO records
		(channel, "offset", id, time, topic, qos, publisher, bridge, expires, properties, published_retained, retained, payload, payload_size)
		SELECT channel, ?, id, time, topic, 2, publisher, bridge, expires, properties, ?, ?, payload, payload_size
		FROM held_publishes WHERE session = ? AND bridge = ? AND packet_id = ?`},
	deleteHeldPublish: {name: "DeleteHeldPublish", write: true, sql: `DELETE FROM held_publishes
		WHERE session = ? AND bridge = ? AND packet_id = ?`},
	heldChannelTotals: {name: "HeldChannelTotals", sql: `SELECT channel,
		coalesce(sum(length(CAST(topic AS BLOB)) + payload_size + coalesce(length(properties), 0)), 0)
		FROM held_publishes GROUP BY channel ORDER BY channel`},
	retainedChargesTotal: {name: "RetainedChargesTotal", sql: `SELECT coalesce(sum(bytes), 0) FROM (
		SELECT topic, max(bytes) AS bytes FROM (
		SELECT topic, length(CAST(topic AS BLOB)) + payload_size + coalesce(length(properties), 0) AS bytes
		FROM records WHERE channel = 1 AND retained = 1
		UNION ALL SELECT topic, length(CAST(topic AS BLOB)) + payload_size + coalesce(length(properties), 0)
		FROM held_publishes WHERE channel = 1 AND retain = 1 AND payload_size > 0)
		GROUP BY topic)`},
	deleteOrphanHeld: {name: "DeleteOrphanHeld", write: true, sql: `DELETE FROM held_publishes
  WHERE session > 0 AND NOT EXISTS (SELECT 1 FROM sessions WHERE id = held_publishes.session)`},
	dueHeld: {name: "DueHeld", sql: `SELECT orphan.session, max(orphan.client), 1, 0, 0, 0, NULL, 0 FROM (
  SELECT session, publisher AS client FROM held_publishes WHERE bridge = 0
  UNION ALL SELECT session, '' AS client FROM session_inflight
  UNION ALL SELECT session, '' AS client FROM share_returned WHERE state = 2) AS orphan
  WHERE orphan.session > ? AND NOT EXISTS (SELECT 1 FROM sessions s WHERE s.id = orphan.session)
  GROUP BY orphan.session ORDER BY orphan.session LIMIT ?`},

	dropRecords:         {name: "DropRecords", write: true, sql: `DELETE FROM records WHERE channel = ?`},
	dropPositions:       {name: "DropPositions", write: true, sql: `DELETE FROM positions WHERE channel = ?`},
	dropStarts:          {name: "DropStarts", write: true, sql: `DELETE FROM subscription_starts WHERE channel = ?`},
	dropSessionInflight: {name: "DropSessionInflight", write: true, sql: `DELETE FROM session_inflight WHERE channel = ?`},
	dropHeld:            {name: "DropHeld", write: true, sql: `DELETE FROM held_publishes WHERE channel = ?`},
	dropJobs:            {name: "DropJobs", write: true, sql: `DELETE FROM queue_jobs WHERE channel = ?`},
	dropShareCursors:    {name: "DropShareCursors", write: true, sql: `DELETE FROM share_cursors WHERE channel = ?`},
	dropShareReturned:   {name: "DropShareReturned", write: true, sql: `DELETE FROM share_returned WHERE channel = ?`},
	dropBridgeCursors:   {name: "DropBridgeCursors", write: true, sql: `DELETE FROM bridge_cursors WHERE channel = ?`},
	dropBridgeInflight:  {name: "DropBridgeInflight", write: true, sql: `DELETE FROM bridge_inflight WHERE channel = ?`},
	dropChannel:         {name: "DropChannel", write: true, sql: `DELETE FROM channels WHERE id = ?`},
	readRetentionRecords: {
		name: "ReadRetentionRecords",
		sql: `SELECT r."offset", r.time, r.topic, r.publisher, r.retained,
			length(CAST(r.topic AS BLOB)) + r.payload_size + coalesce(length(r.properties), 0), r.payload_size,
			j.spent, j.first_attempt, j.last_attempt, j.next_try,
			CASE WHEN j.spent IS NOT NULL THEN substr(r.properties, 1, 4) END,
			CASE WHEN j.spent IS NOT NULL THEN coalesce(length(r.properties), 0) ELSE 0 END, r.bridge
			FROM records r LEFT JOIN queue_jobs j ON ?5 AND j.channel = r.channel AND j."offset" = r."offset"
			WHERE r.channel = ?1 AND r."offset" > ?2 AND r."offset" < ?3
			ORDER BY r."offset" LIMIT ?4`,
	},
	deleteRetentionRecords: {
		name: "DeleteRetentionRecords", write: true,
		sql: `DELETE FROM records WHERE channel = ?1 AND "offset" BETWEEN ?2 AND ?3
			AND (retained = 0 OR channel <> 1) AND ` + retentionDueSQL,
	},
	advanceRetention: {
		name: "AdvanceRetention", write: true,
		sql: `UPDATE channels SET floor = CASE WHEN type IN (2, 3) THEN
			coalesce((SELECT min("offset") FROM records WHERE channel = ?3), next) ELSE ?1 END,
			bytes = bytes + ?2 WHERE id = ?3 RETURNING floor`,
	},
	retentionHasDelivery: {
		name: "RetentionHasDelivery", write: true,
		sql: `SELECT EXISTS (SELECT 1 FROM session_inflight WHERE session = ? AND channel = ? AND "offset" = ? AND share = 0
			AND ((phase IN (1, 2) AND ?) OR (phase = 3 AND moves = 1)))`,
	},
	retentionSubscriptions: {
		name: "RetentionSubscriptions", write: true,
		sql: `SELECT s.id, s.client, sub.filter, sub.no_local, sub.partitions, st.start, p."offset",
			EXISTS (SELECT 1 FROM session_inflight i WHERE i.session = s.id AND i.channel = p.channel AND i.share = 0 AND i.phase IN (1, 2))
			FROM sessions s JOIN positions p ON p.session = s.id AND p.channel = ?
			JOIN subscriptions sub ON sub.session = s.id
			LEFT JOIN subscription_starts st ON st.session = sub.session AND st.filter = sub.filter AND st.channel = p.channel
			WHERE p."offset" <= ? AND (s.id, sub.filter) > (?, ?)
			ORDER BY s.id, sub.filter LIMIT 256`,
	},
	retentionDeliveries: {
		name: "RetentionDeliveries", write: true,
		sql: `SELECT DISTINCT "offset" FROM session_inflight
			WHERE phase IN (1, 2) AND share = 0 AND session = ? AND channel = ? AND "offset" > ? AND "offset" <= ?
			ORDER BY "offset" LIMIT 256`,
	},
	readChannelBytes: {
		name: "ReadChannelBytes",
		sql:  `SELECT id, bytes FROM channels WHERE id > ? ORDER BY id LIMIT 256`,
	},
	readMetricPositions: {
		name: "ReadMetricPositions",
		sql: `SELECT session, channel, "offset" FROM positions
			WHERE (session, channel) > (?, ?) ORDER BY session, channel LIMIT 256`,
	},
	readMetricCounts: {
		name: "ReadMetricCounts",
		sql: `SELECT (SELECT count(*) FROM sessions WHERE left > 0),
			(SELECT count(*) FROM subscriptions sub JOIN sessions s ON s.id = sub.session),
			(SELECT count(*) FROM wills WHERE due IS NOT NULL)`,
	},
	retainedTotals: {
		name: "RetainedTotals",
		sql: `SELECT count(*), coalesce(sum(length(CAST(topic AS BLOB)) + payload_size + coalesce(length(properties), 0)), 0),
			min(expires), min(time) FROM records WHERE channel = 1 AND retained = 1`,
	},
	retainedCharge: {
		name: "RetainedCharge",
		sql: `SELECT coalesce(max(bytes), 0), coalesce(max("offset"), 0) FROM (
			SELECT length(CAST(topic AS BLOB)) + payload_size + coalesce(length(properties), 0) AS bytes, "offset"
			FROM records WHERE channel = max(1, ?2) AND topic = ?1 AND retained = 1
			UNION ALL SELECT length(CAST(topic AS BLOB)) + payload_size + coalesce(length(properties), 0), 0
			FROM held_publishes WHERE ?2 <= 1 AND channel = 1 AND topic = ?1 AND retain = 1 AND payload_size > 0)`,
	},
	clearRetainedTopic: {
		name: "ClearRetainedTopic", write: true,
		sql: `UPDATE records SET retained = 0 WHERE channel = 1 AND topic = ? AND retained = 1
			RETURNING "offset", length(CAST(topic AS BLOB)) + payload_size + coalesce(length(properties), 0)`,
	},
	expireRetained: {
		name: "ExpireRetained", write: true,
		sql: `UPDATE records SET retained = 0 WHERE channel = 1 AND "offset" IN
			(SELECT "offset" FROM records INDEXED BY records_retained WHERE channel = 1 AND retained = 1 AND "offset" > ?
			AND (expires <= ? OR (? > 0 AND time <= ? - ?)) ORDER BY "offset" LIMIT 256)
			RETURNING "offset", length(CAST(topic AS BLOB)) + payload_size + coalesce(length(properties), 0), topic`,
	},
	nextRetainedDeadline: {
		name: "NextRetainedDeadline",
		sql: `SELECT min(deadline) FROM
			(SELECT expires AS deadline FROM records WHERE channel = 1 AND retained = 1 AND expires > ?
			UNION ALL SELECT time + ? FROM records WHERE channel = 1 AND retained = 1 AND ? > 0 AND time + ? > ?)`,
	},
	readRetainedMetadata: {
		name: "ReadRetainedMetadata",
		sql: `SELECT "offset", topic, qos, publisher, expires, properties, payload_size, CASE WHEN length(payload) < payload_size THEN length(payload) ELSE 0 END, published_retained, id, time, bridge
			FROM records WHERE channel = ? AND retained = 1 AND payload_size > 0 AND "offset" >= ? AND "offset" < ?
			ORDER BY "offset" LIMIT ?`,
	},
	loadSessionPosition: {
		name: "LoadSessionPosition",
		sql:  `SELECT channel, "offset", last_seen FROM positions WHERE session = ? ORDER BY channel`,
	},
	loadDeliverySequence: {
		name: "LoadDeliverySequence",
		sql:  `SELECT coalesce(max(seq), 0), count(*) FROM session_inflight WHERE session = ? AND phase IN (1, 2)`,
	},
	readSessionDeliveries: {
		name: "ReadSessionDeliveries",
		sql: `SELECT i.packet_id, i.seq, i.channel, i."offset", i.qos, i.phase = 2, i.moves, i.share, i.attempt, i.attempt_number, i.attempt_began, i.out_retain, i.out_initial,
			CASE WHEN i.phase = 1 THEN i.out_properties END,
			r."offset" IS NULL OR (i."offset" < c.floor AND i.out_initial = 0), coalesce(r.topic, ''), r.properties, r.expires, coalesce(r.payload_size, 0), CASE WHEN length(r.payload) < r.payload_size THEN length(r.payload) ELSE 0 END
			FROM session_inflight i JOIN channels c ON c.id = i.channel LEFT JOIN records r ON i.phase = 1 AND r.channel = i.channel AND r."offset" = i."offset"
			WHERE i.session = ? AND i.seq <= ? AND
			((i.phase = 1 AND i.seq > ? AND ? = 0) OR (i.phase = 2 AND i.seq > ? AND ? = 0))
			ORDER BY i.seq LIMIT ?`,
	},
	nextSessionRelease: {
		name: "NextSessionRelease",
		sql: `SELECT coalesce(min(seq), 0) FROM session_inflight
			WHERE session = ? AND seq > ? AND seq <= ? AND phase = 2`,
	},
	deleteOrphanDeliveries: {
		name: "DeleteOrphanDeliveries", write: true,
		sql: `DELETE FROM session_inflight WHERE (share = 0 OR phase = 3) AND NOT EXISTS
			(SELECT 1 FROM sessions WHERE id = session_inflight.session)`,
	},
	readRecordMetadata: {name: "ReadRecordMetadata", sql: recordMetadataSQL},
	addSessionDelivery: {
		name: "AddSessionDelivery", write: true,
		sql: `INSERT INTO session_inflight (session, packet_id, seq, channel, "offset", qos, phase,
			moves, share, attempt, attempt_number, attempt_began, out_retain, out_initial, out_properties) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?)`,
	},
	settleSessionDelivery: {
		name: "SettleSessionDelivery", write: true,
		sql: `DELETE FROM session_inflight WHERE session = ? AND packet_id = ? AND phase IN (1, 2)`,
	},
	markSessionDelivery: {
		name: "MarkSessionDelivery", write: true,
		sql: `UPDATE session_inflight SET phase = 3 WHERE session = ? AND packet_id = ? AND phase IN (1, 2)`,
	},
	pruneSessionMarkers: {
		name: "PruneSessionMarkers", write: true,
		sql: `DELETE FROM session_inflight WHERE session = ? AND channel = ? AND phase = 3 AND (moves = 0 OR "offset" < ?)`,
	},
	deleteSessionMarkers: {
		name: "DeleteSessionMarkers", write: true,
		sql: `DELETE FROM session_inflight WHERE session = ? AND channel = ? AND phase = 3`,
	},
	releaseSessionDelivery: {
		name: "ReleaseSessionDelivery", write: true,
		sql: `UPDATE session_inflight SET phase = 2 WHERE session = ? AND packet_id = ? AND qos = 2 AND phase = 1`,
	},
	moveSessionPosition: {
		name: "MoveSessionPosition", write: true,
		// Ordinary progress is monotone. A consumer detached by the last
		// UNSUBSCRIBE can finish older exchanges after a new consumer starts;
		// those completions cannot move the new position backwards.
		sql: `UPDATE positions SET "offset" = ?, last_seen = ? WHERE session = ? AND channel = ? AND "offset" < ?`,
	},
	rewindSessionPosition: {
		name: "RewindSessionPosition", write: true,
		sql: `UPDATE positions SET "offset" = ?, last_seen = ? WHERE session = ? AND channel = ?`,
	},
	restoreSessionDelivery: {
		name: "RestoreSessionDelivery", write: true,
		sql: `UPDATE session_inflight SET moves = 1
			WHERE session = ? AND channel = ? AND packet_id = ? AND "offset" >= ? AND moves = 0 AND phase IN (1, 2)`,
	},
	supersedeSessionDeliveries: {
		name: "SupersedeSessionDeliveries", write: true,
		sql: `UPDATE session_inflight SET moves = 0 WHERE session = ? AND channel = ? AND "offset" < ? AND moves = 1 AND phase IN (1, 2)`,
	},
	AddChannel: {
		name: "AddChannel", write: true,
		sql: `INSERT INTO channels (id, name, type, next, floor, bytes) VALUES (?, ?, ?, ?, ?, ?)`,
	},
	AddRecord: {
		name: "AddRecord", write: true,
		sql: `INSERT INTO records (channel, "offset", id, time, topic, qos, publisher, bridge,
			expires, properties, published_retained, retained, payload, payload_size)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	},
	updateLatestRecord: {name: "UpdateLatestRecord", write: true,
		sql: `UPDATE records SET "offset" = ?2, id = ?3, time = ?4, qos = ?6, publisher = ?7,
			bridge = ?8, expires = ?9, properties = ?10, payload = ?13, payload_size = ?14
			WHERE channel = ?1 AND topic = ?5 AND retained = 1`},
	updateLatestHeld: {name: "UpdateLatestHeld", write: true,
		sql: `UPDATE records AS r SET "offset" = ?1, id = h.id, time = h.time, qos = 2,
			publisher = h.publisher, bridge = h.bridge, expires = h.expires, properties = h.properties,
			payload = h.payload, payload_size = h.payload_size
			FROM held_publishes h WHERE h.session = ?4 AND h.bridge = ?5 AND h.packet_id = ?6
			AND r.channel = h.channel AND r.topic = h.topic AND r.retained = 1 AND ?2 = 0 AND ?3 = 1`},
	advanceChannel: {
		name: "AdvanceChannel", write: true,
		sql: `UPDATE channels SET next = max(next, ?), bytes = bytes + ? WHERE id = ?`,
	},
	LoadChannels: {
		name: "LoadChannels",
		sql:  `SELECT id, name, type, next, floor, bytes FROM channels ORDER BY id`,
	},
	countChannelRecords: {
		name: "CountChannelRecords",
		sql:  `SELECT count(*) FROM records WHERE channel = ?`,
	},
	channelBelowSaved: {
		name: "ChannelBelowSaved",
		sql: `SELECT name, next FROM channels c
			WHERE next <= coalesce((SELECT max("offset") FROM records WHERE channel = c.id), 0)
			OR next < coalesce((SELECT max(start) FROM subscription_starts WHERE channel = c.id), 0)
			OR next < coalesce((SELECT max("offset") FROM bridge_cursors WHERE channel = c.id), 0)
			ORDER BY id LIMIT 1`,
	},
	readView: {
		name: "ReadView",
		sql:  `SELECT id FROM channels WHERE id = 1`,
	},
	loadSession: {
		name: "LoadSession",
		sql: `SELECT id, client, user, protocol, expiry, left, receive_maximum, listener, remote, keepalive, clean_start
			FROM sessions WHERE client = ?`,
	},
	addSession: {
		name: "AddSession", write: true,
		sql: `INSERT INTO sessions (client, user, protocol, expiry, left, receive_maximum, listener, remote, keepalive, clean_start, id)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	},
	updateSession: {
		name: "UpdateSession", write: true,
		sql: `UPDATE sessions SET client = ?, user = ?, protocol = ?, expiry = ?, left = ?, receive_maximum = ?,
			listener = ?, remote = ?, keepalive = ?, clean_start = ? WHERE id = ?`,
	},
	leaveSession: {
		name: "LeaveSession", write: true,
		sql: `UPDATE sessions SET expiry = ?, left = ? WHERE id = ?`,
	},
	deleteSession: {
		name: "DeleteSession", write: true,
		sql: `DELETE FROM sessions WHERE id = ?`,
	},
	deleteSubscriptions: {
		name: "DeleteSubscriptions", write: true,
		sql: `DELETE FROM subscriptions WHERE session = ?`,
	},
	deleteWill: {
		name: "DeleteWill", write: true,
		sql: `DELETE FROM wills WHERE session = ?`,
	},
	addWill: {
		name: "AddWill", write: true,
		sql: `INSERT INTO wills (session, client, user, topic, qos, retain, delay, due, payload, payload_size, properties)
			VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)`,
	},
	loadWill: {
		name: "LoadWill",
		sql: `SELECT w.client, w.user, w.topic, w.qos, w.retain, w.delay, w.payload, w.payload_size, w.properties
			FROM wills w LEFT JOIN sessions s ON s.id = w.session WHERE w.session = ?
			AND ` + willMemorySQL + ` = ?
			AND w.delay = ? AND w.due IS ? AND coalesce(s.left, 0) = ?`,
	},
	loadWillInfo: {
		name: "LoadWillInfo",
		sql: `SELECT w.session, ` + willMemorySQL + `,
			w.delay, w.due, coalesce(s.left, 0) FROM wills w LEFT JOIN sessions s ON s.id = w.session WHERE w.session = ?`,
	},
	scheduleWill: {
		name: "ScheduleWill", write: true,
		sql: `UPDATE wills SET due = ? WHERE session = ? AND due IS NULL`,
	},
	dueWills: {
		name: "DueWills",
		// The due bound seeks into wills_due; its implicit rowid orders
		// sessions sharing a deadline without a separate sort.
		sql: `SELECT w.session, w.client, w.due, w.session,
			` + willMemorySQL + `,
			w.delay, w.due, coalesce(s.left, 0)
			FROM wills w LEFT JOIN sessions s ON s.id = w.session
			WHERE w.due >= ? AND (w.due, w.session) > (?, ?)
			ORDER BY w.due, w.session LIMIT ?`,
	},
	retryWills: {
		name: "RetryWills",
		// A refused immediate firing can leave due NULL. Only the retry
		// looks for these candidates, seeking past its last session id.
		sql: `SELECT w.session, w.client, 1, w.session,
			` + willMemorySQL + `,
			w.delay, w.due, coalesce(s.left, 0)
			FROM wills w LEFT JOIN sessions s ON s.id = w.session
			WHERE w.session > ? AND w.due IS NULL AND (w.delay = 0 OR s.id IS NULL)
			ORDER BY w.session LIMIT ?`,
	},
	restoreWills: {
		name: "RestoreWills", write: true,
		sql: `UPDATE wills SET due = CASE WHEN NOT EXISTS (SELECT 1 FROM sessions WHERE id = wills.session)
			THEN ? ELSE ? + delay * 1000 END WHERE due IS NULL
			OR NOT EXISTS (SELECT 1 FROM sessions WHERE id = wills.session)`,
	},
	nextWill: {
		name: "NextWill",
		sql:  `SELECT coalesce(min(due), 0), count(*) FROM wills`,
	},
	deleteSubscriptionStarts: {
		name: "DeleteSubscriptionStarts", write: true,
		sql: `DELETE FROM subscription_starts WHERE session = ?`,
	},
	deletePositions: {
		name: "DeletePositions", write: true,
		sql: `DELETE FROM positions WHERE session = ?`,
	},
	deleteSessionInflight: {
		name: "DeleteSessionInflight", write: true,
		sql: `DELETE FROM session_inflight WHERE session = ?`,
	},
	deleteSessionHeld: {
		name: "DeleteSessionHeld", write: true,
		sql: `DELETE FROM held_publishes WHERE session = ?`,
	},
	dueSessions: {
		name: "DueSessions",
		// The explicit expiry bound lets SQLite seek into sessions_expiry;
		// the pair comparison also orders sessions sharing that expiry.
		sql: fmt.Sprintf(`SELECT s.id, s.client, s.left + s.expiry * 1000,
			coalesce(w.session, 0), coalesce(`+willMemorySQL+`, 0),
			coalesce(w.delay, 0), w.due, CASE WHEN w.session IS NULL THEN 0 ELSE s.left END
			FROM sessions s LEFT JOIN wills w ON w.session = s.id
			WHERE s.left > 0 AND s.expiry <> %d AND s.left + s.expiry * 1000 >= ?
			AND (s.left + s.expiry * 1000, s.id) > (?, ?)
			ORDER BY s.left + s.expiry * 1000, s.id LIMIT ?`, NeverExpires),
	},
	highestSession: {
		name: "HighestSession",
		sql: `SELECT max(coalesce((SELECT max(id) FROM sessions), 0),
			coalesce((SELECT max(session) FROM subscriptions), 0),
			coalesce((SELECT max(session) FROM subscription_starts), 0),
			coalesce((SELECT max(session) FROM wills), 0),
			coalesce((SELECT max(session) FROM positions), 0),
			coalesce((SELECT max(session) FROM session_inflight), 0),
			coalesce((SELECT max(session) FROM held_publishes), 0),
			coalesce((SELECT max(session) FROM share_returned), 0))`,
	},
	leaveConnectedSessions: {
		name: "LeaveConnectedSessions", write: true,
		sql: `UPDATE sessions SET left = ? WHERE left = 0`,
	},
	countRestoredSessions: {
		name: "CountRestoredSessions",
		sql:  fmt.Sprintf(`SELECT count(*) FROM sessions WHERE left = 0 OR expiry = %d OR left + expiry * 1000 > ?`, NeverExpires),
	},
	loadSessionSubscriptions: {
		name: "LoadSessionSubscriptions",
		sql: `SELECT s.filter, s.qos, s.no_local, s.retain_as_published, s.retain_handling, s.identifier,
			s.partitions, st.channel, st.start FROM subscriptions s LEFT JOIN subscription_starts st
			ON st.session = s.session AND st.filter = s.filter WHERE s.session = ? ORDER BY s.filter, st.channel`,
	},
	addSubscription: {
		name: "AddSubscription", write: true,
		sql: `INSERT INTO subscriptions (qos, no_local, retain_as_published, retain_handling, identifier, partitions, session, filter)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
	},
	updateSubscription: {
		name: "UpdateSubscription", write: true,
		sql: `UPDATE subscriptions SET qos = ?, no_local = ?, retain_as_published = ?, retain_handling = ?,
			identifier = ?, partitions = ? WHERE session = ? AND filter = ?`,
	},
	addSubscriptionStart: {
		name: "AddSubscriptionStart", write: true,
		sql: `INSERT INTO subscription_starts (session, filter, channel, start) VALUES (?, ?, ?, ?)`,
	},
	addSubscriptionPosition: {
		name: "AddSubscriptionPosition", write: true,
		sql: `INSERT INTO positions (session, channel, "offset", last_seen) VALUES (?, ?, ?, NULL)`,
	},
	seekSessionPosition: {
		name: "SeekSessionPosition", write: true,
		sql: `INSERT INTO positions (session, channel, "offset", last_seen) VALUES (?, ?, ?, ?)
 ON CONFLICT (session, channel) DO UPDATE SET "offset" = excluded."offset", last_seen = excluded.last_seen`,
	},
	supersedeSeekDeliveries: {
		name: "SupersedeSeekDeliveries", write: true,
		sql: `UPDATE session_inflight SET moves = 0 WHERE session = ? AND channel = ? AND moves = 1 AND phase IN (1, 2)`,
	},
	readSeekPosition: {
		name: "ReadSeekPosition",
		sql: `SELECT floor, next,
 CASE WHEN ? THEN (SELECT time FROM records WHERE channel = channels.id AND "offset" >= channels.floor ORDER BY "offset" LIMIT 1) END,
 CASE WHEN ? THEN (SELECT "offset" FROM records WHERE channel = channels.id AND "offset" >= channels.floor AND time >= ? ORDER BY "offset" LIMIT 1) END
 FROM channels WHERE id = ?`,
	},
	readCurrentValueSize: {
		name: "ReadCurrentValueSize",
		sql: `SELECT payload_size, CASE WHEN length(payload) < payload_size THEN length(payload) ELSE 0 END
 FROM records WHERE channel = ? AND topic = ? AND retained = 1`,
	},
	readCurrentValuePayload: {
		name: "ReadCurrentValuePayload",
		sql:  `SELECT payload FROM records WHERE channel = ? AND topic = ? AND retained = 1`,
	},
	removeSubscription: {
		name: "RemoveSubscription", write: true,
		sql: `DELETE FROM subscriptions WHERE session = ? AND filter = ?`,
	},
	removeSubscriptionStarts: {
		name: "RemoveSubscriptionStarts", write: true,
		sql: `DELETE FROM subscription_starts WHERE session = ? AND filter = ?`,
	},
	removeSubscriptionPosition: {
		name: "RemoveSubscriptionPosition", write: true,
		sql: `DELETE FROM positions WHERE session = ? AND channel = ?`,
	},
	supersedeSubscriptionDeliveries: {
		name: "SupersedeSubscriptionDeliveries", write: true,
		sql: `UPDATE session_inflight SET moves = 0
			WHERE session = ? AND channel = ? AND share = 0 AND attempt IS NULL AND moves = 1 AND phase IN (1, 2)`,
	},
}

// retentionDueSQL deletes only due latest records inside the examined page.
// Append retention supplies false and deletes the prefix selected by its walk.
const retentionDueSQL = `(?4 = 0 OR (payload_size > 0 AND ?5 > 0 AND time <= ?6 - ?7)
	OR (payload_size = 0 AND ?8 > 0 AND time <= ?6 - ?9))`

// recordMetadataRangeSQL gives ordinary and shared scans the same live rows.
const recordMetadataRangeSQL = ` FROM records r
	WHERE r.channel = ?1 AND r."offset" >= ?2 AND r."offset" < ?3
	ORDER BY r."offset" LIMIT ?4`

const recordMetadataFieldsSQL = `SELECT r."offset", r.topic, r.qos, r.publisher, r.expires, r.properties, r.payload_size,
	CASE WHEN length(r.payload) < r.payload_size THEN length(r.payload) ELSE 0 END, r.published_retained, r.id, r.time, r.bridge`

const recordMetadataSQL = recordMetadataFieldsSQL + recordMetadataRangeSQL

// Full write chunks supplement single-row operations. Payload reads prepare
// powers of two so small selections bind fewer than twice their row count.
const (
	addRecordChunk = statementCount + iota
	addQueueJobChunk
	addDeliveryChunk
	addBridgeDeliveryChunk
	settleBridgeDeliveryChunk
	releaseBridgeDeliveryChunk
	settleDeliveryChunk
	releaseDeliveryChunk
	markDeliveryChunk
	terminalSharedChunk
	retireSharedChunk
	readRecordPayloads
	readMetadataDispositions  = readRecordPayloads + metadataShapes
	retainSharedReturnedPages = readMetadataDispositions + metadataShapes - 1
	preparedCount             = retainSharedReturnedPages + metadataShapes - 1 // single-key shapes belong to the operation table
)

// Key shapes share the payload reader's powers of two, through MetadataPage.
const metadataShapes = 9

type preparedStatements [preparedCount]*sql.Stmt

func (p *preparedStatements) get(s Statement) *sql.Stmt {
	if int(s) >= len(p) {
		return nil
	}
	return p[s]
}

// Conn is a lifetime database connection and the statements prepared on it.
// Read operations use it only during ReadConn, inside the reader's transaction.
type Conn struct {
	*sql.Conn
	statements preparedStatements
}

// Prepared returns the connection's statement, or nil if it is unavailable.
// Operations execute it with context.Background and never close it.
func (c *Conn) Prepared(s Statement) *sql.Stmt { return c.statements.get(s) }

// Tx is the writer's operation context: its transaction and prepared statements,
// or its WAL path and prepared statements for work outside a transaction.
// It and its statements may be used only during the WriteTx callback.
type Tx struct {
	*sql.Tx
	walPath          string // immutable; Sync opens the file only for its request
	statements       *preparedStatements
	recordArgs       [recordChunk * recordColumns]any   // bounded row scratch shared by serial write operations
	jobKeys          [recordChunk * 2]any               // queue job keys accompanying those same rows
	deliveryKeys     [deliveryChunk * settleColumns]any // phase keys alongside preparation rows; reused by settlements
	deliverySessions map[int64]struct{}                 // reused, bounded membership scratch for independent delivery runs
}

// Prepared returns a writer statement, or nil if it is unavailable. These
// connection-prepared statements already execute in the open SQLite
// transaction; wrapping them in sql.Tx.StmtContext would prepare them again.
func (t *Tx) Prepared(s Statement) *sql.Stmt { return t.statements.get(s) }

// prepare is called only at startup, after the schema exists. The caller
// closes this connection and any earlier statements if preparation fails.
func (c *Conn) prepare(writes bool) error {
	for id, spec := range &statementSQL {
		if spec.write != writes {
			continue
		}
		stmt, err := c.PrepareContext(context.Background(), spec.sql)
		if err != nil {
			return fmt.Errorf("prepare %s: %w", spec.name, err)
		}
		c.statements[id] = stmt
	}
	if writes {
		for _, id := range [...]Statement{retainedCharge, readRetentionRecords, nextQueueRetry} {
			stmt, err := c.PrepareContext(context.Background(), statementSQL[id].sql)
			if err != nil {
				return fmt.Errorf("prepare %s: %w", statementSQL[id].name, err)
			}
			c.statements[id] = stmt
		}
		for _, chunk := range [...]struct {
			id  Statement
			sql string
		}{
			{addRecordChunk, recordSQL(recordChunk)},
			{addQueueJobChunk, insertSQL(addQueueJob, recordChunk)},
			{addDeliveryChunk, insertSQL(addSessionDelivery, deliveryChunk)},
			{addBridgeDeliveryChunk, insertSQL(addBridgeDelivery, deliveryChunk)},
			{settleBridgeDeliveryChunk, deliveryKeySQL(settleBridgeDelivery)},
			{releaseBridgeDeliveryChunk, deliveryKeySQL(releaseBridgeDelivery)},
			{settleDeliveryChunk, deliveryKeySQL(settleSessionDelivery)},
			{releaseDeliveryChunk, deliveryKeySQL(releaseSessionDelivery)},
			{markDeliveryChunk, deliveryKeySQL(markSessionDelivery)},
			{terminalSharedChunk, deliveryKeySQL(terminalSharedDelivery)},
			{retireSharedChunk, deliveryKeySQL(retireSharedDelivery)},
		} {
			stmt, err := c.PrepareContext(context.Background(), chunk.sql)
			if err != nil {
				return fmt.Errorf("prepare row chunk %d: %w", chunk.id, err)
			}
			c.statements[chunk.id] = stmt
		}
	}
	for i := range metadataShapes {
		shape := Statement(i)
		if !writes {
			query := `SELECT "offset", payload, payload_size FROM records WHERE channel = ? AND "offset" IN (` +
				strings.TrimSuffix(strings.Repeat("?,", 1<<shape), ",") + `) ORDER BY "offset"`
			stmt, err := c.PrepareContext(context.Background(), query)
			if err != nil {
				return fmt.Errorf("prepare payload rows %d: %w", 1<<shape, err)
			}
			c.statements[readRecordPayloads+shape] = stmt
		}
		if shape == 0 {
			continue
		}
		single, shapes, first := readMetadataDisposition, readMetadataDispositions, 4
		if writes {
			single, shapes, first = retainSharedReturned, retainSharedReturnedPages, 3
		}
		stmt, err := c.PrepareContext(context.Background(), metadataKeySQL(single, 1<<shape, first))
		if err != nil {
			return fmt.Errorf("prepare %s rows %d: %w", statementSQL[single].name, 1<<shape, err)
		}
		c.statements[shapes+shape-1] = stmt
	}
	return nil
}

// metadataKeySQL varies only the bounded page keys, preserving each
// operation's predicate and its complete indexed recipient key.
func metadataKeySQL(single Statement, rows, first int) string {
	var values strings.Builder
	for i := range rows {
		if i != 0 {
			values.WriteByte(',')
		}
		_, _ = fmt.Fprintf(&values, "(?%d)", i+first)
	}
	return strings.Replace(statementSQL[single].sql, fmt.Sprintf("VALUES (?%d)", first), "VALUES "+values.String(), 1)
}

// deliveryKeySQL preserves the single-row predicate, batching complete keys
// for both settlements and phase changes across independent sessions.
func deliveryKeySQL(single Statement) string {
	owner := "session"
	if single == settleBridgeDelivery || single == releaseBridgeDelivery {
		owner = "bridge"
	}
	keys := `(` + owner + `, packet_id) IN (VALUES ` + strings.TrimSuffix(strings.Repeat("(?, ?),", deliveryChunk), ",") + ")"
	return strings.Replace(statementSQL[single].sql, owner+" = ? AND packet_id = ?", keys, 1)
}

// recordSQL derives all shapes from the operation table's one column list.
func recordSQL(rows int) string {
	return insertSQL(AddRecord, rows)
}

func insertSQL(statement Statement, rows int) string {
	head, row, _ := strings.Cut(statementSQL[statement].sql, "VALUES ")
	return head + "VALUES " + strings.TrimSuffix(strings.Repeat(row+",", rows), ",")
}

// close releases statements before returning the connection to its database.
// An operation may already have closed a broken connection. Closing its
// statements then only reports that same dead connection; its operation's
// answer carried the error, so cleanup has no new error to report.
func (c *Conn) close() error {
	var err error
	for _, stmt := range c.statements {
		if stmt != nil {
			if closeErr := stmt.Close(); err == nil {
				err = closeErr
			}
		}
	}
	closeErr := c.Close()
	if errors.Is(closeErr, sql.ErrConnDone) {
		return nil
	}
	if err != nil {
		return err
	}
	return closeErr
}
