package time

import (
	"context"
	"log"
	"time"

	"github.com/blindson76/gstate"
)

type TimeMachine struct {
}

func (m TimeMachine) Clone() TimeMachine {
	return m
}

func NewMachine() *gstate.Machine[string, string, TimeMachine] {
	return gstate.New[string, string, TimeMachine]("TimeMachine").
		Initial("init").
		State("init", func(sb *gstate.StateBuilder[string, string, TimeMachine]) {
			sb.On("event").Assign(func(m TimeMachine) TimeMachine {
				log.Printf("preassign\n")
				time.Sleep(3 * time.Second)
				log.Printf("postassign\n")
				return m
			}).GoTo("ready")
			sb.After(time.Second * 3)
			sb.Entry(func(m TimeMachine) TimeMachine {
				log.Printf("Entry init\n")
				return m
			})
			sb.Invoke(func(ctx context.Context, snap TimeMachine, mutate func(func(TimeMachine) TimeMachine)) error {
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
		State("ready", func(sb *gstate.StateBuilder[string, string, TimeMachine]) {
			sb.Entry(func(m TimeMachine) TimeMachine {
				log.Println("enter ready")
				return m
			})
		}).Build()
}
