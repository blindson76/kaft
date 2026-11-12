package main

import (
	"context"
	"log"
	"time"

	"github.com/blindson76/gstate"
)

type Machine struct {
}

func (m Machine) Clone() Machine {
	return m
}

func main() {
	obs := gstate.ObserverFuncs[string, string, Machine]{
		AnyFunc: func(ctx context.Context) {
			// log.Println("any")
		},
		TransitionFunc: func(ctx context.Context, te *gstate.TransitionEvent[string, string, Machine]) {
			log.Printf("Transition: %s->%s with %s\n", te.From, te.To, te.Event)
		},
		EventReceivedFunc: func(ctx context.Context, en *gstate.EventNotice[string, string, Machine]) {
			log.Printf("EventReceived:%s\n", en.Event)
		},
	}
	machine := gstate.New[string, string, Machine]("df").
		Initial("init").
		State("init", func(sb *gstate.StateBuilder[string, string, Machine]) {
			sb.On("event").Assign(func(m Machine) Machine {
				log.Printf("preassign\n")
				time.Sleep(3 * time.Second)
				log.Printf("postassign\n")
				return m
			}).GoTo("ready")
			sb.Entry(func(m Machine) Machine {
				log.Printf("Entry init\n")
				return m
			})
			sb.Invoke(func(ctx context.Context, snap Machine, mutate func(func(Machine) Machine)) error {
				t := time.NewTicker(time.Second)
				for {
					select {
					case <-ctx.Done():
						log.Printf("Exiting state\n")
						return nil
					case <-t.C:
						log.Printf("Tick\n")
					}
				}
			}, "init", "init")
		}).
		State("ready", func(sb *gstate.StateBuilder[string, string, Machine]) {
			sb.Entry(func(m Machine) Machine {
				log.Println("enter ready")
				return m
			})
		}).Build()

	actor := gstate.Start(machine, Machine{}, machine.WithHydrateServices(false), machine.WithObservers(obs))

	// snap := actor.Snapshot()
	// actor.Stop()
	// actor = gstate.Hydrate(machine, snap, machine.WithHydrateServices(true), machine.WithObservers(obs))

	time.Sleep(5 * time.Second)
	log.Printf("pre send")
	// actor.SendWith("event",func(m Machine) Machine {})
	ctx, _ := context.WithTimeout(context.Background(), time.Second*2)
	actor.SendCtx(ctx, "event")
	log.Printf("post send")

	time.Sleep(5 * time.Hour)
}
