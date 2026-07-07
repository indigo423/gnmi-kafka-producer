// SPDX-License-Identifier: Apache-2.0

package dialout

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aristanetworks/goarista/gnmireverse"
	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/tbotnz/gnmi-kafka-producer/internal/config"
)

type sentRecord struct {
	target string
	key    string
	fields map[string]any
}

// chanSink collects Send calls for assertions.
type chanSink struct{ ch chan sentRecord }

func newChanSink() *chanSink { return &chanSink{ch: make(chan sentRecord, 64)} }

func (c *chanSink) Send(_ context.Context, target string, key, value []byte) {
	var fields map[string]any
	_ = json.Unmarshal(value, &fields)
	c.ch <- sentRecord{target: target, key: string(key), fields: fields}
}

func (c *chanSink) next(t *testing.T) sentRecord {
	t.Helper()
	select {
	case r := <-c.ch:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a produced record")
		return sentRecord{}
	}
}

func (c *chanSink) expectNone(t *testing.T) {
	t.Helper()
	select {
	case r := <-c.ch:
		t.Fatalf("unexpected record produced: %+v", r)
	case <-time.After(200 * time.Millisecond):
	}
}

func registry() config.Dialout {
	return config.Dialout{
		Devices: []config.DialoutDevice{
			{Name: "d1", Address: "192.168.100.1", Labels: map[string]string{"role": "leaf"}},
		},
	}
}

// startServer runs a Server on a random port and returns its address. It hands
// Serve the already-bound listener, so there is no close-and-relisten race.
func startServer(t *testing.T, cfg config.Dialout, sink Sink) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan error, 1)
	go func() { done <- New(cfg, sink).Serve(ctx, lis) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("server Serve: %v", err)
		}
	})
	waitReachable(t, addr)
	return addr
}

func waitReachable(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if conn, err := net.Dial("tcp", addr); err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server at %s never became reachable", addr)
}

func dialClient(t *testing.T, addr string, creds credentials.TransportCredentials) gnmireverse.GNMIReverseClient {
	t.Helper()
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return gnmireverse.NewGNMIReverseClient(cc)
}

// counterSample builds a SubscribeResponse carrying one in-octets sample for
// eth0, with the device identity in-band via Prefix.Target (as nl6 does).
func counterSample(target string, octets uint64, ts time.Time) *gnmipb.SubscribeResponse {
	const iface = "eth0"
	return &gnmipb.SubscribeResponse{
		Response: &gnmipb.SubscribeResponse_Update{
			Update: &gnmipb.Notification{
				Timestamp: ts.UnixNano(),
				Prefix:    &gnmipb.Path{Target: target},
				Update: []*gnmipb.Update{{
					Path: &gnmipb.Path{Elem: []*gnmipb.PathElem{
						{Name: "interfaces"},
						{Name: "interface", Key: map[string]string{"name": iface}},
						{Name: "state"},
						{Name: "counters"},
						{Name: "in-octets"},
					}},
					Val: &gnmipb.TypedValue{Value: &gnmipb.TypedValue_UintVal{UintVal: octets}},
				}},
			},
		},
	}
}

func publish(t *testing.T, client gnmireverse.GNMIReverseClient, rsps ...*gnmipb.SubscribeResponse) {
	t.Helper()
	stream, err := client.Publish(context.Background())
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	for _, rsp := range rsps {
		if err := stream.Send(rsp); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
}

func TestPublishAttributesAndEnriches(t *testing.T) {
	sink := newChanSink()
	addr := startServer(t, registry(), sink)
	client := dialClient(t, addr, insecure.NewCredentials())

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	publish(t, client,
		counterSample("192.168.100.1", 1000, base),
		counterSample("192.168.100.1", 2000, base.Add(10*time.Second)),
	)

	first := sink.next(t)
	if first.target != "d1" || first.key != "192.168.100.1|eth0" {
		t.Fatalf("first record target/key = %q/%q, want d1/192.168.100.1|eth0", first.target, first.key)
	}
	for field, want := range map[string]any{
		"target": "d1", "role": "leaf", "device": "192.168.100.1", "interface": "eth0", "in_octets": 1000.0,
	} {
		if got := first.fields[field]; got != want {
			t.Fatalf("first record %s = %v, want %v", field, got, want)
		}
	}
	if _, ok := first.fields["in_octets_bps"]; ok {
		t.Fatal("first record must not carry a rate (no prior sample)")
	}

	second := sink.next(t)
	if got := second.fields["in_octets_bps"]; got != 800.0 { // (1000/10s)*8
		t.Fatalf("second record in_octets_bps = %v, want 800", got)
	}
}

func TestRateSurvivesReconnect(t *testing.T) {
	sink := newChanSink()
	addr := startServer(t, registry(), sink)
	client := dialClient(t, addr, insecure.NewCredentials())

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	publish(t, client, counterSample("192.168.100.1", 1000, base))
	sink.next(t)

	// New stream = the device reconnected. The next sample must rate against
	// the pre-reconnect baseline.
	publish(t, client, counterSample("192.168.100.1", 2000, base.Add(10*time.Second)))
	rec := sink.next(t)
	if got := rec.fields["in_octets_bps"]; got != 800.0 {
		t.Fatalf("post-reconnect in_octets_bps = %v, want 800 (rate state lost with the stream?)", got)
	}
}

func TestUnknownTargetDroppedAndCounted(t *testing.T) {
	sink := newChanSink()
	addr := startServer(t, registry(), sink)
	client := dialClient(t, addr, insecure.NewCredentials())

	before := counterValue(t, "gateway_dialout_unknown_target_total", nil)
	publish(t, client, counterSample("10.9.9.9", 1000, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)))
	sink.expectNone(t)
	if got := counterValue(t, "gateway_dialout_unknown_target_total", nil); got != before+1 {
		t.Fatalf("unknown_target_total = %v, want %v", got, before+1)
	}
}

func TestStreamGaugeAndUpdateCounter(t *testing.T) {
	sink := newChanSink()
	addr := startServer(t, registry(), sink)
	client := dialClient(t, addr, insecure.NewCredentials())

	updatesBefore := counterValue(t, "gateway_dialout_updates_received_total", map[string]string{"target": "d1"})

	stream, err := client.Publish(context.Background())
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := stream.Send(counterSample("192.168.100.1", 1, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))); err != nil {
		t.Fatalf("Send: %v", err)
	}
	sink.next(t) // handler has seen the update, so the stream is open
	if got := gaugeValue(t, "gateway_dialout_streams_active"); got < 1 {
		t.Fatalf("streams_active = %v while a stream is open, want >= 1", got)
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	waitFor(t, "streams_active back to 0", func() bool {
		return gaugeValue(t, "gateway_dialout_streams_active") == 0
	})
	if got := counterValue(t, "gateway_dialout_updates_received_total", map[string]string{"target": "d1"}); got != updatesBefore+1 {
		t.Fatalf("updates_received_total{d1} = %v, want %v", got, updatesBefore+1)
	}
}

func TestTLSListener(t *testing.T) {
	certFile, keyFile, pool := selfSigned(t)
	cfg := registry()
	cfg.TLS = &config.DialoutTLS{CertFile: certFile, KeyFile: keyFile}
	sink := newChanSink()
	addr := startServer(t, cfg, sink)

	t.Run("plaintext client is rejected", func(t *testing.T) {
		client := dialClient(t, addr, insecure.NewCredentials())
		stream, err := client.Publish(context.Background())
		if err == nil {
			_ = stream.Send(counterSample("192.168.100.1", 1, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)))
			_, err = stream.CloseAndRecv()
		}
		if err == nil {
			t.Fatal("plaintext publish against a TLS listener succeeded, want error")
		}
	})

	t.Run("TLS client publishes", func(t *testing.T) {
		creds := credentials.NewTLS(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12})
		client := dialClient(t, addr, creds)
		publish(t, client, counterSample("192.168.100.1", 1000, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)))
		if rec := sink.next(t); rec.target != "d1" {
			t.Fatalf("record target = %q, want d1", rec.target)
		}
	})
}

// selfSigned writes an ephemeral server cert/key for 127.0.0.1 and returns
// their paths plus a pool trusting the cert.
func selfSigned(t *testing.T) (certFile, keyFile string, pool *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "dialout-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		IsCA:         true, BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	dir := t.TempDir()
	certFile = filepath.Join(dir, "server.crt")
	keyFile = filepath.Join(dir, "server.key")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	pool = x509.NewCertPool()
	pool.AppendCertsFromPEM(certPEM)
	return certFile, keyFile, pool
}

// counterValue / gaugeValue read a metric from the default registry (promauto
// registers there), matching the given labels exactly (nil = no labels).
func counterValue(t *testing.T, name string, labels map[string]string) float64 {
	m := findMetric(t, name, labels)
	if m == nil {
		return 0 // counter not yet initialized
	}
	return m.GetCounter().GetValue()
}

func gaugeValue(t *testing.T, name string) float64 {
	m := findMetric(t, name, nil)
	if m == nil {
		return 0
	}
	return m.GetGauge().GetValue()
}

func findMetric(t *testing.T, name string, labels map[string]string) *dto.Metric {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, fam := range families {
		if fam.GetName() != name {
			continue
		}
		for _, m := range fam.GetMetric() {
			got := make(map[string]string, len(m.GetLabel()))
			for _, lp := range m.GetLabel() {
				got[lp.GetName()] = lp.GetValue()
			}
			if len(got) != len(labels) {
				continue
			}
			match := true
			for k, v := range labels {
				if got[k] != v {
					match = false
					break
				}
			}
			if match {
				return m
			}
		}
	}
	return nil
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
