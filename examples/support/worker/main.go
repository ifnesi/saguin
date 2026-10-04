// Command worker is a saguin queue worker written against Eclipse Paho.
//
// There is no saguin code here beyond the two topic strings, and both are
// discoverable: the subscription form is what the broker tells you when it
// refuses anything else, and the response topic arrives on the message
// itself as the MQTT 5 Response Topic property.
//
//	go run ./examples/support/worker -queue jobs -id worker-1
//	go run ./examples/support/worker -queue jobs -id worker-2 -fail
//
// **-queue is the channel's name**, whatever its filter says. A queue
// admits one subscription form and it is `$saguin/queue/` plus that name,
// so a worker needs nothing out of saguin.yaml - the same name spells the
// seek topic, the response topic and the dead-letter channel.
//
// **-window declares a Receive Maximum**: how many deliveries the broker may
// have in flight to this worker at once. Unset by default. It does not change
// how many jobs the worker holds, which is one from each queue at a time
// whatever the window, so on a queue it bounds only what else the connection
// carries.
//
//	for i in $(seq 1 300); do mosquitto_pub -V 5 -t iot/depot/work/$i -m j -q 1; done
//	go run ./examples/support/worker -queue jobs -id w1 -window 20 -work 0s
//
// Against this broker that drains all 300, and the flag exists because it
// did not. A worker declaring a Receive Maximum used to stop being offered
// work at about twice that number and strand its window's worth in
// Delivering: the client's packet identifiers and the server's are
// independent spaces and the substrate kept both in one table, so a
// worker's own answer destroyed the delivery it was answering about
// (mochi-mqtt/server#519, carried on the fork).
//
// **It still reproduces against upstream mochi**, which is why the flag and
// the count below stay rather than being deleted with the defect. Anyone
// measuring it there wants a backlog several times the window: with fewer
// jobs than the window's reach the worker finishes before it can starve and
// everything looks well, which is how four separate measurements of it went
// wrong.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:1883", "broker address")
	queue := flag.String("queue", "jobs",
		"the queue channel's name")
	id := flag.String("id", "worker-1", "MQTT client id")
	fail := flag.Bool("fail", false, "return every job instead of acknowledging it")
	stall := flag.Bool("stall", false, "never answer, so every job hits the visibility timeout")
	work := flag.Duration("work", 200*time.Millisecond, "time spent pretending to work")
	// Unset by default, which is what the demo and the test suite rely on:
	// with no Receive Maximum the window is 65,535 and no backlog reaches
	// it. Setting it is what makes this worker the reproduction.
	window := flag.Uint("window", 0, "Receive Maximum, 1-65535, or 0 to leave it unset; a worker holds one job from a queue at a time whatever it is")
	flag.Parse()

	// Refused rather than truncated. Receive Maximum is a uint16 on the
	// wire, so -window 70000 would have gone out as 4464 and -window 65536
	// as 0 - and zero is a protocol error MQTT forbids sending, which this
	// broker does not currently refuse. An instrument that says one thing
	// and sends another is the failure this whole flag exists to prevent.
	if *window > 65535 {
		log.Fatalf("-window %d is above MQTT's Receive Maximum of 65535", *window)
	}

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}

	// Counted so that a worker that stops being offered work reads as
	// "stopped at 22" rather than as silence, which is indistinguishable
	// from an empty queue. Against this broker it counts to the end of the
	// backlog; against upstream mochi it is the measurement. One goroutine
	// calls the handler, so this needs no lock.
	done := 0

	var c *paho.Client
	cfg := paho.ClientConfig{Conn: conn}
	cfg.OnPublishReceived = []func(paho.PublishReceived) (bool, error){
		func(pr paho.PublishReceived) (bool, error) {
			p := pr.Packet
			attempt, msgID := "?", "?"
			var respTopic string
			var corr []byte
			if p.Properties != nil {
				respTopic = p.Properties.ResponseTopic
				corr = p.Properties.CorrelationData
				for _, u := range p.Properties.User {
					switch u.Key {
					case "saguin-attempt":
						attempt = u.Value
					case "saguin-id":
						msgID = u.Value
					}
				}
			}
			fmt.Printf("[%s] got %s  attempt=%s  id=%s  payload=%q\n",
				*id, p.Topic, attempt, msgID, p.Payload)

			time.Sleep(*work)

			if *stall {
				fmt.Printf("[%s] stalling: not answering, the lease will expire\n", *id)
				return true, nil
			}
			action := "ack"
			if *fail {
				action = "return"
			}
			// The whole application protocol: publish the action to the
			// Response Topic the broker supplied, echoing its Correlation
			// Data. An ordinary MQTT 5 PUBLISH.
			if _, err := c.Publish(context.Background(), &paho.Publish{
				Topic:      respTopic,
				QoS:        1,
				Payload:    []byte(action),
				Properties: &paho.PublishProperties{CorrelationData: corr},
			}); err != nil {
				fmt.Printf("[%s] responding failed: %v\n", *id, err)
				return true, nil
			}
			done++
			fmt.Printf("[%s] %s  (%d done)\n", *id, action, done)
			return true, nil
		},
	}
	c = paho.NewClient(cfg)

	expiry := uint32(3600)
	props := &paho.ConnectProperties{SessionExpiryInterval: &expiry}
	// prefetch is what went on the wire, not what was typed, because those
	// are the two things worth being able to tell apart.
	prefetch := "unset"
	if *window > 0 {
		rx := uint16(*window)
		props.ReceiveMaximum = &rx
		prefetch = fmt.Sprintf("%d", rx)
	}
	ca, err := c.Connect(context.Background(), &paho.Connect{
		ClientID:   *id,
		KeepAlive:  30,
		CleanStart: true,
		Properties: props,
	})
	if err != nil || ca.ReasonCode != 0 {
		log.Fatalf("connect: %v (reason %v)", err, ca)
	}

	// **`$saguin/queue/` and the queue's name.** One queue admits one exact
	// string, so two spellings of the same intent would be two populations
	// of workers that each take a copy of every job - which is why every
	// other spelling is refused rather than made to work. The name is what a
	// seek topic, a response topic and a dead-letter channel are made of,
	// and a worker needs nothing out of its operator's configuration file to
	// consume.
	filter := "$saguin/queue/" + *queue
	sa, err := c.Subscribe(context.Background(), &paho.Subscribe{
		Subscriptions: []paho.SubscribeOptions{{Topic: filter, QoS: 1}},
	})
	if err != nil && sa == nil {
		log.Fatalf("subscribe %s: %v", filter, err)
	}
	if sa.Reasons[0] > 2 {
		log.Fatalf("subscribe %s refused: %#x", filter, sa.Reasons[0])
	}
	fmt.Printf("[%s] consuming %s  receive maximum %s\n", *id, filter, prefetch)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	_ = c.Disconnect(&paho.Disconnect{ReasonCode: 0})
}
