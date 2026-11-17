package internal

import (
	"context"
	"net"
	"slices"
	"strings"
	"testing"

	"github.com/miekg/dns"
)

const dnsConfig = `
expected_bootstrap: 1
mongo: {bin: mongod}
kafka: {home: k}
dns:
  listen: 127.0.0.1:0
nodes:
  - id: s1
    roles: [server]
    raft_addr: 10.0.0.1:7001
    http_addr: 10.0.0.1:8001
  - id: w1
    roles: [worker]
    controllers: [mongo]
    http_addr: 10.0.0.11:8001
    mongo_addr: 10.0.0.11:27017
    dns_names: [db-main]
  - id: w2
    roles: [worker]
    http_addr: 10.0.0.12:8001
    mongo_addr: 10.0.0.12:27017
    kafka_id: 1
    kafka_addr: 10.0.0.12:9092
    kafka_controller_addr: 10.0.0.12:9093
  - id: w3
    roles: [worker]
    controllers: [kafka]
    ip: fd00::13
    http_addr: kaft-w3.lan:8001
    kafka_id: 2
    kafka_addr: 10.0.0.13:9092
    kafka_controller_addr: 10.0.0.13:9093
`

func TestDNSRecords(t *testing.T) {
	cfg, err := ParseConfigBytes([]byte(dnsConfig))
	if err != nil {
		t.Fatal(err)
	}
	recs, err := buildDNSRecords(cfg, "service.")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{
		"s1.service.":      {"10.0.0.1"},
		"w1.service.":      {"10.0.0.11"},
		"db-main.service.": {"10.0.0.11"},
		"mongo1.service.":  {"10.0.0.11"},
		"mongo2.service.":  {"10.0.0.12"},
		"mongo.service.":   {"10.0.0.11", "10.0.0.12"},
		"kafka1.service.":  {"10.0.0.12"},
		"kafka2.service.":  {"fd00::13"},
		"kafka.service.":   {"10.0.0.12", "fd00::13"},
	}
	for name, hosts := range want {
		if !slices.Equal(recs[name], hosts) {
			t.Errorf("%s = %v, want %v", name, recs[name], hosts)
		}
	}
}

func TestDNSRejectsDuplicateNames(t *testing.T) {
	cfg, err := ParseConfigBytes([]byte(strings.Replace(dnsConfig, "dns_names: [db-main]", "dns_names: [kafka1]", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := buildDNSRecords(cfg, "service."); err == nil || !strings.Contains(err.Error(), "kafka1.service. is defined twice") {
		t.Fatalf("err = %v", err)
	}
}

func TestDNSServer(t *testing.T) {
	cfg, err := ParseConfigBytes([]byte(dnsConfig))
	if err != nil {
		t.Fatal(err)
	}
	// A forwarder for names outside the zone.
	upstream := &dns.Server{PacketConn: listenUDP(t), Handler: dns.HandlerFunc(func(w dns.ResponseWriter, r *dns.Msg) {
		m := new(dns.Msg).SetReply(r)
		m.Answer = []dns.RR{&dns.A{Hdr: dns.RR_Header{Name: r.Question[0].Name, Rrtype: dns.TypeA, Class: dns.ClassINET}, A: net.IPv4(1, 2, 3, 4)}}
		_ = w.WriteMsg(m)
	})}
	go func() { _ = upstream.ActivateAndServe() }()
	t.Cleanup(func() { _ = upstream.Shutdown() })
	cfg.DNS.Forward = []string{upstream.PacketConn.LocalAddr().String()}

	// Find a free port for UDP and TCP alike.
	pc := listenUDP(t)
	addr := pc.LocalAddr().String()
	pc.Close()
	cfg.DNS.Listen = addr
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := StartDNS(ctx, cfg, "s1"); err != nil {
		t.Fatal(err)
	}

	ask := func(network, name string, qtype uint16) *dns.Msg {
		t.Helper()
		c := &dns.Client{Net: network}
		resp, _, err := c.Exchange(new(dns.Msg).SetQuestion(dns.Fqdn(name), qtype), addr)
		if err != nil {
			t.Fatalf("%s %s: %v", network, name, err)
		}
		return resp
	}
	values := func(m *dns.Msg) []string {
		var out []string
		for _, rr := range m.Answer {
			switch r := rr.(type) {
			case *dns.A:
				out = append(out, r.A.String())
			case *dns.AAAA:
				out = append(out, r.AAAA.String())
			}
		}
		return out
	}

	if got := values(ask("udp", "MONGO1.service", dns.TypeA)); !slices.Equal(got, []string{"10.0.0.11"}) {
		t.Errorf("mongo1 A = %v", got)
	}
	if got := values(ask("tcp", "mongo.service", dns.TypeA)); !slices.Equal(got, []string{"10.0.0.11", "10.0.0.12"}) {
		t.Errorf("mongo A over tcp = %v", got)
	}
	if got := values(ask("udp", "kafka2.service", dns.TypeAAAA)); !slices.Equal(got, []string{"fd00::13"}) {
		t.Errorf("kafka2 AAAA = %v", got)
	}
	if m := ask("udp", "kafka2.service", dns.TypeA); m.Rcode != dns.RcodeSuccess || len(m.Answer) != 0 {
		t.Errorf("kafka2 A: rcode %d, %d answers; want an empty NOERROR", m.Rcode, len(m.Answer))
	}
	if m := ask("udp", "mongo9.service", dns.TypeA); m.Rcode != dns.RcodeNameError || !m.Authoritative {
		t.Errorf("unknown name: rcode %d aa=%v, want authoritative NXDOMAIN", m.Rcode, m.Authoritative)
	}
	if got := values(ask("udp", "example.org", dns.TypeA)); !slices.Equal(got, []string{"1.2.3.4"}) {
		t.Errorf("forwarded = %v", got)
	}
}

func listenUDP(t *testing.T) net.PacketConn {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return pc
}
