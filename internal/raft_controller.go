package internal

import (
	"context"
	"log"
	"sync"
	"time"
)

type ClusterManager interface {
	Bootstrap(context.Context, []string) error
	Join(string)
	WaitNode(context.Context, string)
}

type ClusterBootstrapController struct {
	expected         int
	nodes            []string
	bootstrapDelay   time.Duration
	c                ClusterManager
	bootstrappedOnce sync.Once
	bootstrappedCh   chan struct{}
}

func NewClusterManager(nodes []string, expected int, bootstrapDelay time.Duration, controller ClusterManager) *ClusterBootstrapController {
	return &ClusterBootstrapController{
		c:              controller,
		expected:       expected,
		nodes:          nodes,
		bootstrapDelay: bootstrapDelay,
		bootstrappedCh: make(chan struct{}),
	}
}

func (rc *ClusterBootstrapController) NotifyBootstraped() {
	rc.bootstrappedOnce.Do(func() {
		close(rc.bootstrappedCh)
	})
}
func (rc *ClusterBootstrapController) Start(ctx context.Context) error {
	internalCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	ch := make(chan string, len(rc.nodes))
	var wg sync.WaitGroup

	for _, node := range rc.nodes {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()

			rc.c.WaitNode(internalCtx, n)

			if internalCtx.Err() != nil {
				return
			}
			ch <- n
		}(node)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	activeNodes := make([]string, 0, len(rc.nodes))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case node, ok := <-ch:
			if !ok {
				return nil
			}

			activeNodes = append(activeNodes, node)

			if len(activeNodes) == rc.expected {
				log.Printf("Bootstrap in %v ...", rc.bootstrapDelay)
				t := time.NewTimer(rc.bootstrapDelay)
				defer t.Stop()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-rc.bootstrappedCh:
					log.Printf("Already bootstraped, Skipping bootstrap...")
					break
				case <-t.C:
					if err := rc.c.Bootstrap(ctx, rc.nodes); err != nil {
						return err
					}
				}
			} else if len(activeNodes) > rc.expected {
				rc.c.Join(node)
			}
		}
	}
}
