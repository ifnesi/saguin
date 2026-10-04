// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package mqtt

import (
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ifnesi/saguin/internal/mqtt/packets"
	"github.com/ifnesi/saguin/internal/sourcetree"

	"github.com/stretchr/testify/require"
)

type modifiedHookBase struct {
	HookBase
	err    error
	fail   bool
	failAt int
}

var errTestHook = errors.New("error")

func (h *modifiedHookBase) ID() string {
	return "modified"
}

func (h *modifiedHookBase) Init(config any) error {
	if config != nil {
		return errTestHook
	}
	return nil
}

func (h *modifiedHookBase) Provides(b byte) bool {
	return true
}

func (h *modifiedHookBase) Stop() error {
	if h.fail {
		return errTestHook
	}

	return nil
}

func (h *modifiedHookBase) OnConnect(cl *Client, pk packets.Packet) error {
	if h.fail {
		return errTestHook
	}

	return nil
}

func (h *modifiedHookBase) OnConnectAuthenticate(cl *Client, pk packets.Packet) bool {
	return true
}

func (h *modifiedHookBase) OnACLCheck(cl *Client, topic string, write bool) bool {
	return true
}

func (h *modifiedHookBase) OnPublish(cl *Client, pk packets.Packet) (packets.Packet, error) {
	if h.fail {
		if h.err != nil {
			return pk, h.err
		}

		return pk, errTestHook
	}

	return pk, nil
}

func (h *modifiedHookBase) OnPacketRead(cl *Client, pk packets.Packet) (packets.Packet, error) {
	if h.fail {
		if h.err != nil {
			return pk, h.err
		}

		return pk, errTestHook
	}

	return pk, nil
}

func (h *modifiedHookBase) OnWill(cl *Client, will Will) (Will, error) {
	if h.fail {
		return will, errTestHook
	}

	return will, nil
}

func TestHooksAddLenGetAll(t *testing.T) {
	h := new(Hooks)
	err := h.Add(new(HookBase), nil)
	require.NoError(t, err)

	err = h.Add(new(modifiedHookBase), nil)
	require.NoError(t, err)

	require.Equal(t, int64(2), h.qty.Load())
	require.Equal(t, int64(2), h.Len())

	all := h.GetAll()
	require.Equal(t, "base", all[0].ID())
	require.Equal(t, "modified", all[1].ID())
}

func TestHooksAddInitFailure(t *testing.T) {
	h := new(Hooks)
	err := h.Add(new(modifiedHookBase), map[string]any{})
	require.Error(t, err)
	require.Equal(t, int64(0), h.qty.Load())
}

func TestHooksStop(t *testing.T) {
	h := new(Hooks)
	h.Log = logger

	err := h.Add(new(HookBase), nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), h.qty.Load())
	require.Equal(t, int64(1), h.Len())

	h.Stop()
}

// coverage: also cover some empty functions
func TestHooksNonReturns(t *testing.T) {
	h := new(Hooks)
	cl := new(Client)

	for i := 0; i < 2; i++ {
		t.Run("step-"+strconv.Itoa(i), func(t *testing.T) {
			// on first iteration, check without hook methods
			h.OnStarted()
			h.OnSessionEstablish(cl, packets.Packet{})
			h.OnSessionEstablished(cl, packets.Packet{})
			h.OnDisconnect(cl, nil, false)
			h.OnConnectRefused(cl, packets.Packet{}, packets.ErrServerBusy)
			h.OnPacketSent(cl, packets.Packet{}, []byte{})
			h.OnSubscribed(cl, packets.Packet{}, []byte{1})
			h.OnUnsubscribed(cl, packets.Packet{})
			h.OnPublishDropped(cl, packets.Packet{})
			h.OnRetainMessage(cl, packets.Packet{}, 0)
			h.OnQosPublish(cl, packets.Packet{}, time.Now().Unix(), 0)
			h.OnQosComplete(cl, packets.Packet{})
			h.OnQosDropped(cl, packets.Packet{})
			h.OnClientExpired(cl)

			// on second iteration, check added hook methods
			err := h.Add(new(modifiedHookBase), nil)
			require.NoError(t, err)
		})
	}
}

func TestHooksOnConnectAuthenticate(t *testing.T) {
	h := new(Hooks)

	ok := h.OnConnectAuthenticate(new(Client), packets.Packet{})
	require.False(t, ok)

	err := h.Add(new(modifiedHookBase), nil)
	require.NoError(t, err)

	ok = h.OnConnectAuthenticate(new(Client), packets.Packet{})
	require.True(t, ok)
}

func TestHooksOnACLCheck(t *testing.T) {
	h := new(Hooks)

	ok := h.OnACLCheck(new(Client), "a/b/c", true)
	require.False(t, ok)

	err := h.Add(new(modifiedHookBase), nil)
	require.NoError(t, err)

	ok = h.OnACLCheck(new(Client), "a/b/c", true)
	require.True(t, ok)
}

func TestHooksOnSubscribe(t *testing.T) {
	h := new(Hooks)
	err := h.Add(new(modifiedHookBase), nil)
	require.NoError(t, err)

	pki := packets.Packet{
		Filters: packets.Subscriptions{
			{Filter: "a/b/c", Qos: 1},
		},
	}
	pk := h.OnSubscribe(new(Client), pki)
	require.EqualValues(t, pk, pki)
}

func TestHooksOnSelectSubscribers(t *testing.T) {
	h := new(Hooks)
	err := h.Add(new(modifiedHookBase), nil)
	require.NoError(t, err)

	subs := &Subscribers{
		Subscriptions: map[string]packets.Subscription{
			"cl1": {Filter: "a/b/c"},
		},
	}

	subs2 := h.OnSelectSubscribers(subs, packets.Packet{})
	require.EqualValues(t, subs, subs2)
}

func TestHooksOnUnsubscribe(t *testing.T) {
	h := new(Hooks)
	err := h.Add(new(modifiedHookBase), nil)
	require.NoError(t, err)

	pki := packets.Packet{
		Filters: packets.Subscriptions{
			{Filter: "a/b/c", Qos: 1},
		},
	}

	pk := h.OnUnsubscribe(new(Client), pki)
	require.EqualValues(t, pk, pki)
}

func TestHooksOnPublish(t *testing.T) {
	h := new(Hooks)
	h.Log = logger

	hook := new(modifiedHookBase)
	err := h.Add(hook, nil)
	require.NoError(t, err)

	pk, err := h.OnPublish(new(Client), packets.Packet{PacketID: 10})
	require.NoError(t, err)
	require.Equal(t, uint16(10), pk.PacketID)

	// coverage: failure
	hook.fail = true
	pk, err = h.OnPublish(new(Client), packets.Packet{PacketID: 10})
	require.Error(t, err)
	require.Equal(t, uint16(10), pk.PacketID)

	// coverage: reject packet
	hook.err = packets.ErrRejectPacket
	pk, err = h.OnPublish(new(Client), packets.Packet{PacketID: 10})
	require.Error(t, err)
	require.ErrorIs(t, err, packets.ErrRejectPacket)
	require.Equal(t, uint16(10), pk.PacketID)
}

func TestHooksOnPublishLogsAReasonCodeAtDebug(t *testing.T) {
	var buf bytes.Buffer
	h := new(Hooks)
	h.Log = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	hook := new(modifiedHookBase)
	err := h.Add(hook, nil)
	require.NoError(t, err)

	// A reason code is a decision the server answers with, not a fault.
	hook.fail = true
	hook.err = packets.ErrQuotaExceeded
	_, err = h.OnPublish(new(Client), packets.Packet{PacketID: 10})
	require.ErrorIs(t, err, packets.ErrQuotaExceeded)
	require.NotContains(t, buf.String(), "level=ERROR")
	require.Contains(t, buf.String(), "level=DEBUG")
	require.Contains(t, buf.String(), "reason=\"quota exceeded\"")

	// Anything that is not a reason code the server can answer with is
	// still an error.
	buf.Reset()
	hook.err = nil
	_, err = h.OnPublish(new(Client), packets.Packet{PacketID: 10})
	require.ErrorIs(t, err, errTestHook)
	require.Contains(t, buf.String(), "level=ERROR")
}

func TestHooksOnPacketRead(t *testing.T) {
	h := new(Hooks)
	h.Log = logger

	hook := new(modifiedHookBase)
	err := h.Add(hook, nil)
	require.NoError(t, err)

	pk, err := h.OnPacketRead(new(Client), packets.Packet{PacketID: 10})
	require.NoError(t, err)
	require.Equal(t, uint16(10), pk.PacketID)

	// coverage: failure
	hook.fail = true
	pk, err = h.OnPacketRead(new(Client), packets.Packet{PacketID: 10})
	require.NoError(t, err)
	require.Equal(t, uint16(10), pk.PacketID)

	// coverage: reject packet
	hook.err = packets.ErrRejectPacket
	pk, err = h.OnPacketRead(new(Client), packets.Packet{PacketID: 10})
	require.Error(t, err)
	require.ErrorIs(t, err, packets.ErrRejectPacket)
	require.Equal(t, uint16(10), pk.PacketID)
}

func TestHooksOnConnect(t *testing.T) {
	h := new(Hooks)
	h.Log = logger

	hook := new(modifiedHookBase)
	err := h.Add(hook, nil)
	require.NoError(t, err)

	err = h.OnConnect(new(Client), packets.Packet{PacketID: 10})
	require.NoError(t, err)

	hook.fail = true
	err = h.OnConnect(new(Client), packets.Packet{PacketID: 10})
	require.Error(t, err)
}

func TestHooksOnPacketEncode(t *testing.T) {
	h := new(Hooks)
	h.Log = logger

	hook := new(modifiedHookBase)
	err := h.Add(hook, nil)
	require.NoError(t, err)

	pk := h.OnPacketEncode(new(Client), packets.Packet{PacketID: 10})
	require.Equal(t, uint16(10), pk.PacketID)
}

func TestHooksOnLWT(t *testing.T) {
	h := new(Hooks)
	h.Log = logger

	hook := new(modifiedHookBase)
	err := h.Add(hook, nil)
	require.NoError(t, err)

	lwt := h.OnWill(new(Client), Will{TopicName: "a/b/c"})
	require.Equal(t, "a/b/c", lwt.TopicName)

	// coverage: fail lwt
	hook.fail = true
	lwt = h.OnWill(new(Client), Will{TopicName: "a/b/c"})
	require.Equal(t, "a/b/c", lwt.TopicName)
}

func TestHookBaseID(t *testing.T) {
	h := new(HookBase)
	require.Equal(t, "base", h.ID())
}

func TestHookBaseProvidesNone(t *testing.T) {
	h := new(HookBase)
	require.False(t, h.Provides(OnConnect))
	require.False(t, h.Provides(OnDisconnect))
}

func TestHookBaseInit(t *testing.T) {
	h := new(HookBase)
	require.Nil(t, h.Init(nil))
}

func TestHookBaseSetOpts(t *testing.T) {
	h := new(HookBase)
	h.SetOpts(logger, new(HookOptions))
	require.NotNil(t, h.Log)
	require.NotNil(t, h.Opts)
}

func TestHookBaseClose(t *testing.T) {
	h := new(HookBase)
	require.Nil(t, h.Stop())
}

func TestHookBaseOnConnectAuthenticate(t *testing.T) {
	h := new(HookBase)
	v := h.OnConnectAuthenticate(new(Client), packets.Packet{})
	require.False(t, v)
}

func TestHookBaseOnACLCheck(t *testing.T) {
	h := new(HookBase)
	v := h.OnACLCheck(new(Client), "topic", true)
	require.False(t, v)
}

func TestHookBaseOnConnect(t *testing.T) {
	h := new(HookBase)
	err := h.OnConnect(new(Client), packets.Packet{})
	require.NoError(t, err)
}

func TestHookBaseOnPublish(t *testing.T) {
	h := new(HookBase)
	pk, err := h.OnPublish(new(Client), packets.Packet{PacketID: 10})
	require.NoError(t, err)
	require.Equal(t, uint16(10), pk.PacketID)
}

func TestHookBaseOnPacketRead(t *testing.T) {
	h := new(HookBase)
	pk, err := h.OnPacketRead(new(Client), packets.Packet{PacketID: 10})
	require.NoError(t, err)
	require.Equal(t, uint16(10), pk.PacketID)
}

func TestHookBaseOnLWT(t *testing.T) {
	h := new(HookBase)
	lwt, err := h.OnWill(new(Client), Will{TopicName: "a/b/c"})
	require.NoError(t, err)
	require.Equal(t, "a/b/c", lwt.TopicName)
}

// legacyHook implements the Hook interface the way an embedder did before
// this release: WITHOUT embedding HookBase, and knowing nothing of
// OnConnectRefused.
//
// Its method set is exactly the interface's. If a later change adds a method
// to Hook, this file stops compiling - which is the point. A minor release
// must not break an embedder's build, and nothing else in the tree would
// notice if one did.
type legacyHook struct{}

func (h *legacyHook) ID() string                                               { return "" }
func (h *legacyHook) Provides(b byte) bool                                     { return false }
func (h *legacyHook) Init(config any) error                                    { return nil }
func (h *legacyHook) Stop() error                                              { return nil }
func (h *legacyHook) SetOpts(l *slog.Logger, o *HookOptions)                   {}
func (h *legacyHook) OnStarted()                                               {}
func (h *legacyHook) OnConnectAuthenticate(cl *Client, pk packets.Packet) bool { return false }
func (h *legacyHook) OnACLCheck(cl *Client, topic string, write bool) bool     { return false }
func (h *legacyHook) OnConnect(cl *Client, pk packets.Packet) error            { return nil }
func (h *legacyHook) OnSessionEstablish(cl *Client, pk packets.Packet) error   { return nil }
func (h *legacyHook) OnSessionEstablished(cl *Client, pk packets.Packet)       {}
func (h *legacyHook) OnDisconnect(cl *Client, err error, expire bool)          {}
func (h *legacyHook) OnPacketRead(cl *Client, pk packets.Packet) (packets.Packet, error) {
	return packets.Packet{}, nil
}
func (h *legacyHook) OnPacketEncode(cl *Client, pk packets.Packet) packets.Packet {
	return packets.Packet{}
}
func (h *legacyHook) OnPacketSent(cl *Client, pk packets.Packet, b []byte) {}
func (h *legacyHook) OnSubscribe(cl *Client, pk packets.Packet) packets.Packet {
	return packets.Packet{}
}
func (h *legacyHook) OnSubscribed(cl *Client, pk packets.Packet, reasonCodes []byte) {}
func (h *legacyHook) OnSelectSubscribers(subs *Subscribers, pk packets.Packet) *Subscribers {
	return nil
}
func (h *legacyHook) OnUnsubscribe(cl *Client, pk packets.Packet) packets.Packet {
	return packets.Packet{}
}
func (h *legacyHook) OnUnsubscribed(cl *Client, pk packets.Packet) {}
func (h *legacyHook) OnPublish(cl *Client, pk packets.Packet) (packets.Packet, error) {
	return packets.Packet{}, nil
}
func (h *legacyHook) OnPublishDropped(cl *Client, pk packets.Packet)                      {}
func (h *legacyHook) OnRetainMessage(cl *Client, pk packets.Packet, r int64)              {}
func (h *legacyHook) OnQosPublish(cl *Client, pk packets.Packet, sent int64, resends int) {}
func (h *legacyHook) OnQosComplete(cl *Client, pk packets.Packet)                         {}
func (h *legacyHook) OnQosDropped(cl *Client, pk packets.Packet)                          {}
func (h *legacyHook) OnWill(cl *Client, will Will) (Will, error)                          { return Will{}, nil }
func (h *legacyHook) OnClientExpired(cl *Client)                                          {}

// A hook written before OnConnectRefused existed still satisfies Hook, so an
// embedder who never embedded HookBase keeps compiling across this release.
func TestAHookWithoutHookBaseStillSatisfiesTheInterface(t *testing.T) {
	var h Hook = new(legacyHook)
	require.Equal(t, "", h.ID())

	// And the dispatcher steps over it rather than reaching for a method it
	// does not have.
	hooks := new(Hooks)
	require.NoError(t, hooks.Add(new(legacyHook), nil))
	hooks.OnConnectRefused(new(Client), packets.Packet{}, packets.ErrServerBusy)
}

// **No OnPacketSent keeps the bytes it is handed.** They are the encode
// buffer, which goes back to the pool as the hook returns and is encoded
// into by the next write (encodeBufs), so a hook that kept them would read
// another packet. Walks the syntax tree of every Go file in the repository,
// tests included: in every OnPacketSent with a receiver, the bytes are
// unnamed, blank, unused, or only handed on to another OnPacketSent.
func TestNoOnPacketSentKeepsItsBytes(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	var scanned, hooks, forwarded int
	var keeps []string
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if sourcetree.Outside(root, path, d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		scanned++
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, path, nil, 0)
		require.NoError(t, err, path)
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil || fn.Name.Name != "OnPacketSent" || fn.Body == nil {
				continue
			}
			hooks++
			params := fn.Type.Params.List
			last := params[len(params)-1]
			if len(last.Names) == 0 || last.Names[len(last.Names)-1].Name == "_" {
				continue
			}
			bytesParam := last.Names[len(last.Names)-1].Obj
			// Every use of the bytes: an argument to an OnPacketSent call
			// is handing them on, and anything else is keeping them.
			handedOn := map[*ast.Ident]bool{}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if c, ok := n.(*ast.CallExpr); ok {
					if sel, ok := c.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "OnPacketSent" {
						for _, a := range c.Args {
							if id, ok := a.(*ast.Ident); ok && id.Obj == bytesParam {
								handedOn[id] = true
								forwarded++
							}
						}
					}
				}
				if id, ok := n.(*ast.Ident); ok && id.Obj == bytesParam && !handedOn[id] {
					rel, _ := filepath.Rel(root, path)
					keeps = append(keeps, fmt.Sprintf("%s:%d", rel, fset.Position(id.Pos()).Line))
				}
				return true
			})
		}
		return nil
	})
	require.NoError(t, err)
	t.Logf("%d Go files examined, %d OnPacketSent hooks, %d handing their bytes on", scanned, hooks, forwarded)
	require.Greater(t, scanned, 100, "the walk is not reaching the repository")
	// Hooks' dispatcher, HookBase and the broker's own at least; the
	// dispatcher is the one that hands them on.
	require.GreaterOrEqual(t, hooks, 3, "the walk has stopped finding OnPacketSent")
	require.GreaterOrEqual(t, forwarded, 1, "the dispatcher no longer hands the bytes on, so the walk has stopped matching it")
	require.Empty(t, keeps, "an OnPacketSent uses the bytes it is handed, which are reused once it returns; "+
		"only handing them on is allowed here, so a new use has to be shown not to keep them")
}
