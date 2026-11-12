package time

import (
	"log"
	"time"

	"github.com/blindson76/gstate"
)

type TimeMachine struct {
	counter int
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
				time.Sleep(3 * time.Second)
				log.Printf("postassign\n")
				return m
			}).GoTo("ready")
			sb.After(time.Second * 5).Raise("timeout")
			sb.Entry(func(m TimeMachine) TimeMachine {
				log.Printf("Entry init\n")
				return m
			})
			sb.On("tick").Assign(func(tm TimeMachine) TimeMachine {
				log.Printf("receive tick event: %v", tm)
				tm.counter++
				return tm
			})
		}).
		State("ready", func(sb *gstate.StateBuilder[string, string, TimeMachine]) {
			sb.Entry(func(m TimeMachine) TimeMachine {
				log.Println("enter ready")
				return m
			})
		}).Build()
}
