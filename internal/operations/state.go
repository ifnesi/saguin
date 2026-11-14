package operations

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/ifnesi/saguin/internal/config"
	"github.com/ifnesi/saguin/internal/operations/routes"
	"github.com/ifnesi/saguin/internal/operations/views"
	"github.com/ifnesi/saguin/internal/passwd"
)

func (l *Listener) stateView(r *http.Request) (body any, code int) {
	route := routes.Index(r.URL.Path)
	switch route {
	case routes.Config:
		sections := r.URL.Query()["section"]
		for _, section := range sections {
			if !slices.Contains([]string{"bridges", "channels", "limits", "listeners", "operations", "storage"}, section) {
				return nil, http.StatusBadRequest
			}
		}
		output, err := l.config.OperationsOutput(sections)
		if err != nil {
			return unavailable()
		}
		return output, http.StatusOK
	case routes.Users:
		passwords := l.config.Passwords
		if l.live != nil {
			passwords = l.live.Access().Passwords
		}
		return l.users(passwords), http.StatusOK
	}
	result := l.askCore(r, &views.Request{Route: route,
		Channel: strings.TrimPrefix(r.URL.Path, routes.Paths[routes.Queues]+"/")})
	return result.Body, result.Code
}

// askCore uses the declared Health / actions channel for both kinds of call.
// Closing Done publishes abandonment before its coalescing notification. If
// the handover is full, an already queued HTTP event will observe it instead.
func (l *Listener) askCore(r *http.Request, request *views.Request) (result views.Result) {
	// One deadline covers channel admission, the core and every store read.
	// A timer and explicit cancellation use no helper goroutine (R1).
	ctx, cancel := context.WithCancel(r.Context())
	admitted, completed := false, false
	defer func() {
		cancel()
		if admitted && !completed {
			select {
			case l.health <- nil:
			default:
			}
		}
	}()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	request.Done, request.Reply = ctx.Done(), make(chan views.Result, 1)
	result.Body, result.Code = unavailable()
	select {
	case l.health <- request:
		admitted = true
	case <-timer.C:
		return result
	case <-ctx.Done():
		return result
	}
	select {
	case result = <-request.Reply:
		completed = true
		return result
	case <-timer.C:
		return result
	case <-ctx.Done():
		return result
	}
}

func unavailable() (body any, code int) {
	return struct {
		Status string `json:"status"`
	}{"unavailable"}, http.StatusServiceUnavailable
}

type userList struct {
	views.Counts
	Users            []string `json:"users"`
	AnonymousAllowed bool     `json:"anonymous_allowed"`
}

type listenerUsers struct {
	userList
	Certificate   string `json:"certificate"`
	ProxyIdentity bool   `json:"proxy_identity"`
}

type usersView struct {
	userList
	Listeners map[string]listenerUsers `json:"listeners"`
}

func listUsers(file *passwd.File, anonymous bool) userList {
	names := []string{}
	if file != nil {
		names = file.Users()
		slices.Sort(names)
	}
	total := len(names)
	names = names[:min(total, views.Limit)]
	for i := range names {
		names[i] = views.Text(names[i])
	}
	return userList{Total: total, Returned: len(names), Users: names, AnonymousAllowed: anonymous}
}

func (l *Listener) users(passwords map[string]*passwd.File) usersView {
	mqtt := &l.config.Broker.MQTT
	result := usersView{userList: listUsers(passwords[mqtt.PasswordFile], mqtt.AllowAnonymous),
		Listeners: make(map[string]listenerUsers)}
	for _, listeners := range [][]config.Listener{mqtt.Listen.TCP, mqtt.Listen.WS, mqtt.Listen.Unix} {
		for _, listener := range listeners {
			certificate := "none"
			if listener.TLS != nil && listener.TLS.ClientCAFile != "" {
				certificate = "accepted"
				if listener.TLS.RequireCertificate {
					certificate = "required"
				}
			}
			result.Listeners[listener.Name] = listenerUsers{userList: listUsers(passwords[listener.PasswordFile], listener.AllowAnonymous),
				Certificate: certificate, ProxyIdentity: listener.ProxyProtocol}
		}
	}
	return result
}
