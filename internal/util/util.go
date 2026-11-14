package util

import (
	"context"
	"net"
	"time"
)

// ProbeTCP reports whether address accepts a TCP connection within timeout.
func ProbeTCP(ctx context.Context, address string, timeout time.Duration) bool {
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

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
