package sessiontest_test

// The harness lives in internal/brokertest, so that these files can be
// split into several test packages and run in parallel - which is the
// whole point of moving it. These aliases are why that move cost no test
// body a single line: the tests read exactly as they did in internal/broker,
// and a reader of any test still sees `connect`, `start` and `client`
// rather than a package qualifier on every line. Only the names this
// package's tests use are here.
//
// **The mutable knobs are not here.** A `var x = brokertest.X` alias would
// copy the value rather than track it, so the handful of settings a test
// writes - the queue bound, the expiries, the ceilings - are qualified at
// their sites instead.

import "github.com/ifnesi/saguin/internal/brokertest"

var awaitStoredPosition = brokertest.AwaitStoredPosition

type client = brokertest.Client

var connect = brokertest.Connect

var connectWithWill = brokertest.ConnectWithWill
var connectWithWillExpiring = brokertest.ConnectWithWillExpiring
var connectWithWillProps = brokertest.ConnectWithWillProps

var dial = brokertest.Dial

var gone = brokertest.Gone

type harness = brokertest.Harness

var kill = brokertest.Kill
var legacy = brokertest.Legacy
var legacyWithDefault = brokertest.LegacyWithDefault

var metricValue = brokertest.MetricValue
var mqttPacket = brokertest.MqttPacket

var sessionGone = brokertest.SessionGone
var sessionGoneAfter = brokertest.SessionGoneAfter
var settle = brokertest.Settle
var start = brokertest.Start
var startDurable = brokertest.StartDurable
var startDurableSQLite = brokertest.StartDurableSQLite
var startLoggingDebug = brokertest.StartLoggingDebug
var startLoggingDebugSQLite = brokertest.StartLoggingDebugSQLite
var startRetaining = brokertest.StartRetaining
var startTrimming = brokertest.StartTrimming
var tcpOnly = brokertest.TCPOnly

var waitForBacklog = brokertest.WaitForBacklog
var waitUntil = brokertest.WaitUntil
var willConnectBytes = brokertest.WillConnectBytes
var willConnectWithExpiry = brokertest.WillConnectWithExpiry
var willResumeWithExpiry = brokertest.WillResumeWithExpiry

var eachProvider = brokertest.EachProvider
var inChannel = brokertest.InChannel
var qos2Dial = brokertest.QoS2Dial

type qos2Wire = brokertest.QoS2Wire

var operationsAt = brokertest.OperationsAt
var scrapeGauges = brokertest.ScrapeGauges
var windowDial = brokertest.WindowDial
var denyFeatures = brokertest.DenyFeatures
var named = brokertest.Named
var eachSessionProvider = brokertest.EachSessionProvider

type delivered = brokertest.Delivered

var deafReader = brokertest.DeafReader
var numbered = brokertest.Numbered
