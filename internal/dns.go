package internal

import (
	"cmp"
	"context"
	"fmt"
	"log"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/miekg/dns"
)

// DNS names, all in the configured zone ("service" by default):
//
//	<node id>.service        the node
//	<controller><n>.service  the n-th node running that controller's worker,
//	                         counted from 1 in config order: mongo1, kafka2...
//	<controller>.service     every node running that controller's worker
//	<name>.service           any of a node's dns_names
//
// A name resolves to the node's ip, or the host of its raft_addr /
// http_addr. A host that is not an IP address is answered with a CNAME.

// dnsRecords maps fully qualified, lower-case names to node hosts.
type dnsRecords map[string][]string

// buildDNSRecords derives the zone's names from the config.
func buildDNSRecords(cfg *ClusterConfig, zone string) (dnsRecords, error) {
	recs := dnsRecords{}
	owner := map[string]string{} // name -> what defined it, for clear errors
	add := func(name, what string, hosts ...string) error {
		label := strings.ToLower(name)
		if _, ok := dns.IsDomainName(label); !ok || strings.Contains(label, ".") || label == "" {
			return fmt.Errorf("%s: %q is not a valid DNS label", what, name)
		}
		fqdn := label + "." + zone
		if prev, ok := owner[fqdn]; ok {
			return fmt.Errorf("DNS name %s is defined twice: by %s and by %s", fqdn, prev, what)
		}
		owner[fqdn] = what
		recs[fqdn] = hosts
		return nil
	}
	host := func(n NodeConfig) string { return cmp.Or(n.IP, n.Host()) }

	for _, n := range cfg.Nodes {
		if host(n) == "" {
			return nil, fmt.Errorf("node %s has no address for DNS (set ip)", n.ID)
		}
		if err := add(n.ID, "node "+n.ID, host(n)); err != nil {
			return nil, err
		}
		for _, name := range n.DNSNames {
			if err := add(name, "dns_names of node "+n.ID, host(n)); err != nil {
				return nil, err
			}
		}
	}
	for _, controller := range cfg.Enabled() {
		var all []string
		for i, n := range cfg.WorkersOf(controller) {
			name := controller + strconv.Itoa(i+1)
			if err := add(name, controller+" worker "+n.ID, host(n)); err != nil {
				return nil, err
			}
			all = append(all, host(n))
		}
		if len(all) > 0 {
			if err := add(controller, controller+" workers", all...); err != nil {
				return nil, err
			}
		}
	}
	return recs, nil
}

// dnsServer answers for the zone and forwards everything else.
type dnsServer struct {
	zone    string // fully qualified, lower case, e.g. "service."
	ttl     uint32
	records dnsRecords
	forward []string
	client  *dns.Client
}

func newDNSServer(cfg *ClusterConfig) (*dnsServer, error) {
	d := cfg.DNS
	zone := dns.Fqdn(strings.ToLower(strings.Trim(cmp.Or(d.Zone, "service"), ".")))
	ttl := d.TTL
	if ttl <= 0 {
		ttl = 10 * time.Second
	}
	recs, err := buildDNSRecords(cfg, zone)
	if err != nil {
		return nil, err
	}
	s := &dnsServer{zone: zone, ttl: uint32(ttl / time.Second), records: recs, client: &dns.Client{Timeout: 2 * time.Second}}
	for _, f := range d.Forward {
		if _, _, err := net.SplitHostPort(f); err != nil {
			f = net.JoinHostPort(f, "53")
		}
		s.forward = append(s.forward, f)
	}
	return s, nil
}

// StartDNS runs this node's DNS server, on UDP and TCP, if the config enables
// it. It fails if the config's names are inconsistent or the port is taken.
func StartDNS(ctx context.Context, cfg *ClusterConfig, nodeID string) error {
	self, _ := cfg.Node(nodeID)
	listen := cmp.Or(self.DNSListen, cfg.DNS.Listen)
	if listen == "" {
		return nil
	}
	s, err := newDNSServer(cfg)
	if err != nil {
		return err
	}
	for _, network := range []string{"udp", "tcp"} {
		srv := &dns.Server{Addr: listen, Net: network, Handler: s}
		started := make(chan error, 1)
		srv.NotifyStartedFunc = func() { started <- nil }
		go func() { started <- srv.ListenAndServe() }()
		if err := <-started; err != nil {
			return fmt.Errorf("dns %s %s: %w", network, listen, err)
		}
		go func() {
			<-ctx.Done()
			_ = srv.Shutdown()
		}()
	}
	names := make([]string, 0, len(s.records))
	for name := range s.records {
		names = append(names, strings.TrimSuffix(name, "."))
	}
	slices.Sort(names)
	log.Printf("DNS server on %s for zone %s: %s", listen, strings.TrimSuffix(s.zone, "."), strings.Join(names, " "))
	return nil
}

func (s *dnsServer) inZone(name string) bool {
	return name == s.zone || strings.HasSuffix(name, "."+s.zone)
}

func (s *dnsServer) ServeDNS(w dns.ResponseWriter, req *dns.Msg) {
	if len(req.Question) != 1 {
		m := new(dns.Msg)
		_ = w.WriteMsg(m.SetRcode(req, dns.RcodeFormatError))
		return
	}
	q := req.Question[0]
	name := strings.ToLower(q.Name)
	if !s.inZone(name) {
		_ = w.WriteMsg(s.forwardQuery(req))
		return
	}

	m := new(dns.Msg)
	m.SetReply(req)
	m.Authoritative = true
	hosts, ok := s.records[name]
	if !ok {
		if name != s.zone {
			m.Rcode = dns.RcodeNameError
		}
		m.Ns = []dns.RR{s.soa()}
		_ = w.WriteMsg(m)
		return
	}
	for _, host := range hosts {
		if rr := s.answer(q, host); rr != nil {
			m.Answer = append(m.Answer, rr)
		}
	}
	if len(m.Answer) == 0 {
		m.Ns = []dns.RR{s.soa()} // the name exists, not with this type
	}
	_ = w.WriteMsg(m)
}

// answer returns the record for one host of q's name, or nil if the host has
// nothing of the asked type.
func (s *dnsServer) answer(q dns.Question, host string) dns.RR {
	hdr := func(t uint16) dns.RR_Header {
		return dns.RR_Header{Name: q.Name, Rrtype: t, Class: dns.ClassINET, Ttl: s.ttl}
	}
	ip := net.ParseIP(host)
	any := q.Qtype == dns.TypeANY
	switch {
	case ip == nil:
		return &dns.CNAME{Hdr: hdr(dns.TypeCNAME), Target: dns.Fqdn(host)}
	case ip.To4() != nil && (q.Qtype == dns.TypeA || any):
		return &dns.A{Hdr: hdr(dns.TypeA), A: ip.To4()}
	case ip.To4() == nil && (q.Qtype == dns.TypeAAAA || any):
		return &dns.AAAA{Hdr: hdr(dns.TypeAAAA), AAAA: ip}
	}
	return nil
}

func (s *dnsServer) soa() dns.RR {
	return &dns.SOA{
		Hdr:     dns.RR_Header{Name: s.zone, Rrtype: dns.TypeSOA, Class: dns.ClassINET, Ttl: s.ttl},
		Ns:      "ns." + s.zone,
		Mbox:    "hostmaster." + s.zone,
		Serial:  1,
		Refresh: 3600, Retry: 600, Expire: 86400, Minttl: s.ttl,
	}
}

// forwardQuery passes a query outside the zone to the forwarders.
func (s *dnsServer) forwardQuery(req *dns.Msg) *dns.Msg {
	for _, upstream := range s.forward {
		if resp, _, err := s.client.Exchange(req, upstream); err == nil && resp != nil {
			return resp
		}
	}
	m := new(dns.Msg)
	if len(s.forward) == 0 {
		return m.SetRcode(req, dns.RcodeRefused)
	}
	return m.SetRcode(req, dns.RcodeServerFailure)
}
