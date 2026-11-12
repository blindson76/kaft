package util

import (
	"context"
	"net"
	"time"
)

func WaitTCP(ctx context.Context, address string, retry time.Duration) {
	ticker := time.NewTicker(retry)
	defer ticker.Stop()

	var dialer net.Dialer

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			conn, err := dialer.DialContext(ctx, "tcp", address)
			if err == nil {
				conn.Close()
				return
			}
		}
	}
}
