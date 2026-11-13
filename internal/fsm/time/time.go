package time

import (
	"context"
	"log"
	"time"

	"github.com/blindson76/gstate"
)

type TimeMachine struct {
	Counter int
}

func (m TimeMachine) Clone() TimeMachine {
	return m
}

func NewMachine() *gstate.Machine[string, string, TimeMachine] {
	return gstate.New[string, string, TimeMachine]("TimeMachine").
		Initial("init").
		State("init", func(sb *gstate.StateBuilder[string, string, TimeMachine]) {
			sb.On("timeout").Assign(func(m TimeMachine) TimeMachine {
				log.Printf("preassign\n")
				log.Printf("postassign\n")
				return m
			}).GoTo("ready")
			sb.After(time.Second * 5000).Raise("timeout")
			sb.Entry(func(m TimeMachine) TimeMachine {
				log.Printf("Entry init\n")
				return m
			})
			sb.On("tick").Assign(func(tm TimeMachine) TimeMachine {
				log.Printf("receive tick event: %v", tm)
				tm.Counter++
				return tm
			})
			sb.Invoke(func(ctx context.Context, snap TimeMachine, mutate func(func(TimeMachine) TimeMachine)) error {
				log.Printf("ACTIVATING INVOKE")

				ticker := time.NewTicker(200 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-ticker.C:
						log.Println("ticker")
						gstate.Raise(ctx, "tick")
					case <-ctx.Done():
						log.Println("exit")
						return ctx.Err()

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
