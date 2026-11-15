package timectl

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"time"
)

// ntpEpoch is the NTP timestamp origin.
var ntpEpoch = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)

// queryTime asks an NTP server for the time with one SNTP request. server is
// a host, or host:port for a non-standard port. It fails unless the server
// answers and says it is synchronized.
func queryTime(server string, timeout time.Duration) (time.Time, error) {
	if _, _, err := net.SplitHostPort(server); err != nil {
		server = net.JoinHostPort(server, "123")
	}
	conn, err := net.DialTimeout("udp", server, timeout)
	if err != nil {
		return time.Time{}, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))

	req := make([]byte, 48)
	req[0] = 0x23 // no leap warning, version 4, client
	sent := time.Now()
	if _, err := conn.Write(req); err != nil {
		return time.Time{}, err
	}
	resp := make([]byte, 48)
	n, err := conn.Read(resp)
	if err != nil {
		return time.Time{}, err
	}
	trip := time.Since(sent)

	if n < 48 || resp[0]&7 != 4 {
		return time.Time{}, errors.New("not an NTP server reply")
	}
	if leap, stratum := resp[0]>>6, resp[1]; leap == 3 || stratum == 0 || stratum > 15 {
		return time.Time{}, fmt.Errorf("server not synchronized (leap %d, stratum %d)", leap, stratum)
	}
	secs := binary.BigEndian.Uint32(resp[40:44])
	frac := binary.BigEndian.Uint32(resp[44:48])
	at := ntpEpoch.Add(time.Duration(secs)*time.Second + time.Duration(uint64(frac)*1e9>>32))
	// The reply left the server about half a round trip ago.
	return at.Add(trip / 2), nil
}
