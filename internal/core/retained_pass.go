package core

import (
	"cmp"
	"slices"

	"github.com/ifnesi/saguin/internal/store"
)

// retainedKey identifies one filter's current-value pass on one channel.
type retainedKey struct {
	channel int64
	filter  string
}

// retainedFilter records the unfinished range owed by one subscription.
// Subscription changes can add a fresh range while other filters' passes
// wait for send quota. One entry per filter and channel bounds this state.
type retainedFilter struct {
	channel int64
	store.Subscription
	from, before int64 // before is its SUBSCRIBE's live-start boundary
}

// retainedPass reads the union of its filters' unfinished ranges. Its scan
// is independent of live progress; neither is restored after a restart.
type retainedPass struct {
	filters []retainedFilter
	before  int64 // the highest boundary; the first read waits until it is resolved
	scan    int64
	waiting bool // an eligible value could not be selected within the send window
}

func newRetainedPass(filters map[retainedKey]retainedFilter) *retainedPass {
	if len(filters) == 0 {
		return nil
	}
	p := &retainedPass{filters: make([]retainedFilter, 0, len(filters))}
	for key := range filters {
		// Copy directly into the pass, then inspect it in place rather than
		// copying these options through a large range value as well.
		p.filters = append(p.filters, filters[key])
		filter := &p.filters[len(p.filters)-1]
		p.before = max(p.before, filter.before)
		if p.scan == 0 || filter.from < p.scan {
			p.scan = filter.from
		}
	}
	slices.SortFunc(p.filters, func(a, b retainedFilter) int {
		if order := cmp.Compare(a.channel, b.channel); order != 0 {
			return order
		}
		return cmp.Compare(a.Filter, b.Filter)
	})
	return p
}

// reconcileRetained keeps only unfinished ranges still subscribed, using the
// current options for deliveries not yet selected. Advancing each range to
// the old scan prevents a new filter's pass from replaying an earlier one.
func (u *consumerState) reconcileRetained() {
	if u.pass != nil {
		// Selected deliveries retain their original eligibility until hand-off.
		p := *u.pass
		p.filters = slices.Clone(p.filters)
		p.waiting = false // changed filters can remove the value that held the pass
		p.filters = slices.DeleteFunc(p.filters, func(f retainedFilter) bool {
			_, held := u.conn.subscriptions[f.Filter]
			return !held || max(p.scan, f.from) >= f.before
		})
		for i := range p.filters {
			f := &p.filters[i]
			f.from = max(f.from, p.scan)
			f.Subscription = u.conn.subscriptions[f.Filter]
		}
		if len(p.filters) == 0 {
			u.pass = nil
		} else {
			u.pass = &p
		}
	}
}

// startRetainedPass joins a SUBSCRIBE's ranges or a permission-held
// preparation to the unfinished pass. A nonzero from resumes at the cancelled
// selection. It needs no store request: queueConsumer holds the first read
// until every publication below the boundary is resolved.
func (c *Core) startRetainedPass(s *connectionState, pass *retainedPass, from int64) {
	for i := 0; i < len(pass.filters); {
		channel := pass.filters[i].channel
		u := s.consumer(channel)
		filters := make(map[retainedKey]retainedFilter)
		if u.pass != nil {
			for j := range u.pass.filters {
				filter := &u.pass.filters[j]
				filters[retainedKey{channel, filter.Filter}] = *filter
			}
		}
		for i < len(pass.filters) && pass.filters[i].channel == channel {
			filter := pass.filters[i]
			filter.from = max(filter.from, from)
			filter.Subscription = s.subscriptions[filter.Filter]
			if filter.from < filter.before {
				key := retainedKey{channel, filter.Filter}
				if before, exists := filters[key]; exists {
					filter.from = min(filter.from, before.from)
					filter.before = max(filter.before, before.before)
				}
				filters[key] = filter
			}
			i++
		}
		u.pass = newRetainedPass(filters)
		c.queueConsumer(u)
	}
}
