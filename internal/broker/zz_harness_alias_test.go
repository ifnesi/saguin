package broker_test

// The harness lives in internal/brokertest, so that these files can be
// split into several test packages and run in parallel - which is the
// whole point of moving it. These aliases are why that move cost no test
// body a single line: 2,210 call sites read exactly as they always have,
// and a reader of any test still sees `connect`, `start` and `client`
// rather than a package qualifier on every line.
//
// **The mutable knobs are not here.** A `var x = brokertest.X` alias would
// copy the value rather than track it, so the handful of settings a test
// writes - the queue bound, the expiries, the ceilings - are qualified at
// their sites instead.

import "github.com/ifnesi/saguin/internal/brokertest"

var ackAll = brokertest.AckAll
var ackReason = brokertest.AckReason
var admitting311 = brokertest.Admitting311

type allowEverything = brokertest.AllowEverything

var askDisconnect = brokertest.AskDisconnect
var assertNoForgery = brokertest.AssertNoForgery
var awaitCount = brokertest.AwaitCount
var awaitLeased = brokertest.AwaitLeased
var awaitPacket = brokertest.AwaitPacket
var awaitProp = brokertest.AwaitProp
var awaitPuback = brokertest.AwaitPuback
var awaitRegistered = brokertest.AwaitRegistered
var awaitScrape = brokertest.AwaitScrape
var awaitStoredPosition = brokertest.AwaitStoredPosition
var awaitFloorPast = brokertest.AwaitFloorPast
var backlogPayloads = brokertest.BacklogPayloads
var bridgeConfig = brokertest.BridgeConfig

const catalogueReply = brokertest.CatalogueReply
const channelKVGet = brokertest.ChannelKVGet

type client = brokertest.Client

var clientCert = brokertest.ClientCert
var clientCertFrom = brokertest.ClientCertFrom
var collect = brokertest.Collect
var collectPublishes = brokertest.CollectPublishes
var collectUntil = brokertest.CollectUntil
var connackProperties = brokertest.ConnackProperties
var connackPropertiesAsking = brokertest.ConnackPropertiesAsking
var connect = brokertest.Connect
var connect2 = brokertest.Connect2
var connectAs = brokertest.ConnectAs
var connectAsking = brokertest.ConnectAsking
var connectDurable = brokertest.ConnectDurable
var connectExpiring = brokertest.ConnectExpiring
var connectMaxPacket = brokertest.ConnectMaxPacket
var connectNamed = brokertest.ConnectNamed
var connectRx = brokertest.ConnectRx

var connectWithWill = brokertest.ConnectWithWill
var connectWithWillExpiring = brokertest.ConnectWithWillExpiring
var connectWithWillProps = brokertest.ConnectWithWillProps

type countingTarget = brokertest.CountingTarget
type denyEverything = brokertest.DenyEverything

var dial = brokertest.Dial
var dialAsking = brokertest.DialAsking
var dialAt = brokertest.DialAt
var dialProps = brokertest.DialProps
var dialSession = brokertest.DialSession
var dialThrough = brokertest.DialThrough
var dialUpstream = brokertest.DialUpstream
var dialWithUser = brokertest.DialWithUser
var dialWS = brokertest.DialWS
var distinct = brokertest.Distinct
var drain = brokertest.Drain
var esp32 = brokertest.ESP32

type espDevice = brokertest.ESPDevice

var fannedOut = brokertest.FannedOut
var forceCloseLegacy = brokertest.ForceCloseLegacy
var gone = brokertest.Gone

type harness = brokertest.Harness

var holdOneJobPerQueue = brokertest.HoldOneJobPerQueue
var kill = brokertest.Kill
var legacy = brokertest.Legacy
var legacyWithDefault = brokertest.LegacyWithDefault
var limits = brokertest.Limits
var linkCut = brokertest.LinkCut
var acknowledged = brokertest.Acknowledged
var awaitInFlight = brokertest.AwaitInFlight

type lockedWriter = brokertest.LockedWriter

var logValue = brokertest.LogValue

const maxAttempts = brokertest.MaxAttempts
const memStorage = brokertest.MemStorage

var metricValue = brokertest.MetricValue
var migrateToSnapshots = brokertest.MigrateToSnapshots
var migrateToSQLite = brokertest.MigrateToSQLite
var mqttPacket = brokertest.MqttPacket
var newProxy = brokertest.NewProxy
var newTestWriter = brokertest.NewTestWriter
var payloads = brokertest.Payloads

type propClient = brokertest.PropClient
type proxy = brokertest.Proxy
type pubackClock = brokertest.PubackClock

var publishWithAlias = brokertest.PublishWithAlias
var readCatalogue = brokertest.ReadCatalogue
var readRawPacket = brokertest.ReadRawPacket

type received = brokertest.Received

var refusalConnect = brokertest.RefusalConnect
var refusalDial = brokertest.RefusalDial
var refusalRecord = brokertest.RefusalRecord
var refusedRouteOn = brokertest.RefusedRouteOn
var refusedRows = brokertest.RefusedRows
var requirePasswords = brokertest.RequirePasswords
var sendRawConnect = brokertest.SendRawConnect
var sessionGone = brokertest.SessionGone
var sessionGoneAfter = brokertest.SessionGoneAfter
var settle = brokertest.Settle
var shortSocketDir = brokertest.ShortSocketDir
var start = brokertest.Start
var startAdmitting311 = brokertest.StartAdmitting311
var startBackingOff = brokertest.StartBackingOff
var startBounded = brokertest.StartBounded
var startDurable = brokertest.StartDurable
var startDurableSQLite = brokertest.StartDurableSQLite
var startExpiring = brokertest.StartExpiring
var startExpiringOnSQLite = brokertest.StartExpiringOnSQLite
var startLogging = brokertest.StartLogging
var startLoggingDebug = brokertest.StartLoggingDebug
var startLoggingSQLite = brokertest.StartLoggingSQLite
var startNested = brokertest.StartNested
var startOn = brokertest.StartOn
var startQueueOnly = brokertest.StartQueueOnly
var startRandom = brokertest.StartRandom
var startRandomSQLite = brokertest.StartRandomSQLite
var startRetaining = brokertest.StartRetaining
var startRetainingDurably = brokertest.StartRetainingDurably
var startSoaking = brokertest.StartSoaking
var startTrimming = brokertest.StartTrimming
var startUpstream = brokertest.StartUpstream
var startWith = brokertest.StartWith
var startWithoutLatest = brokertest.StartWithoutLatest
var subscribeCode = brokertest.SubscribeCode
var subscriptionIdentifiersIn = brokertest.SubscriptionIdentifiersIn
var tcpOnly = brokertest.TCPOnly
var threeOneOneAdmitted = brokertest.ThreeOneOneAdmitted
var uint32Ptr = brokertest.Uint32Ptr

const visibility = brokertest.Visibility

var waitForBacklog = brokertest.WaitForBacklog
var waitForBridge = brokertest.WaitForBridge
var waitForBridgeGone = brokertest.WaitForBridgeGone
var waitUntil = brokertest.WaitUntil
var willConnectBytes = brokertest.WillConnectBytes
var willConnectWithExpiry = brokertest.WillConnectWithExpiry
var willDelivered = brokertest.WillDelivered
var writeACL = brokertest.WriteACL
var writeClientPair = brokertest.WriteClientPair

type wsConn = brokertest.WSConn

var eachProvider = brokertest.EachProvider
var inChannel = brokertest.InChannel
var qos2Dial = brokertest.QoS2Dial

type qos2Wire = brokertest.QoS2Wire

var operationsAt = brokertest.OperationsAt
var scrapeGauges = brokertest.ScrapeGauges
var windowDial = brokertest.WindowDial
var denyFeatures = brokertest.DenyFeatures
var eachSessionProvider = brokertest.EachSessionProvider

type delivered = brokertest.Delivered

var deafReader = brokertest.DeafReader
