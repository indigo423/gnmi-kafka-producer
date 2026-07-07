// SPDX-License-Identifier: Apache-2.0

// Package dialout implements the gateway's gNMIReverse collector: a gRPC
// server that accepts device-initiated Publish streams
// (Publish(stream gnmi.SubscribeResponse)) and feeds their notifications into
// the same enrichment → Kafka pipeline the dial-in path uses. Devices are
// attributed in-band via Notification.Prefix.Target (nl6 sets it to the
// device's management IP), matched against the dialout.devices registry.
package dialout

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/aristanetworks/goarista/gnmireverse"
	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/tbotnz/gnmi-kafka-producer/internal/config"
	gnmix "github.com/tbotnz/gnmi-kafka-producer/internal/gnmi"
	"github.com/tbotnz/gnmi-kafka-producer/internal/metrics"
)

// Sink is where enriched records go; *kafka.Producer satisfies it.
type Sink interface {
	Send(ctx context.Context, target string, key, value []byte)
}

// unknownLogInterval rate-limits the unknown-target log line: the counter
// tracks every drop, the log samples the offending value.
const unknownLogInterval = 10 * time.Second

// device pairs a registry entry with its Enricher. The Enricher is not safe
// for concurrent use and holds per-device rate state, so it lives here (one
// per device, created once) rather than per stream — rates survive a device's
// reconnect. mu serializes the brief overlap window where an old and a new
// stream from the same device both deliver.
type device struct {
	entry    config.DialoutDevice
	mu       sync.Mutex
	enricher *gnmix.Enricher
}

// Server implements gnmireverse.GNMIReverseServer.
type Server struct {
	gnmireverse.UnimplementedGNMIReverseServer

	cfg     config.Dialout
	sink    Sink
	devices map[string]*device // keyed by registry address (= Prefix.Target)

	unknownMu      sync.Mutex
	lastUnknownLog time.Time
}

func New(cfg config.Dialout, sink Sink) *Server {
	devices := make(map[string]*device, len(cfg.Devices))
	for _, d := range cfg.Devices {
		devices[d.Address] = &device{entry: d, enricher: gnmix.NewEnricher(d.StaticFields())}
	}
	return &Server{cfg: cfg, sink: sink, devices: devices}
}

// Run serves the listener until ctx is cancelled, then drains gracefully.
// It blocks; run it in a goroutine.
func (s *Server) Run(ctx context.Context) error {
	lis, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return err
	}
	var opts []grpc.ServerOption
	if s.cfg.TLS != nil {
		creds, err := credentials.NewServerTLSFromFile(s.cfg.TLS.CertFile, s.cfg.TLS.KeyFile)
		if err != nil {
			return err
		}
		opts = append(opts, grpc.Creds(creds))
	}
	gs := grpc.NewServer(opts...)
	gnmireverse.RegisterGNMIReverseServer(gs, s)
	go func() {
		<-ctx.Done()
		gs.GracefulStop()
	}()
	log.Printf("dialout: listening on %s (tls=%v, devices=%d)", s.cfg.Listen, s.cfg.TLS != nil, len(s.devices))
	if err := gs.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return err
	}
	return nil
}

// Publish consumes one device's stream of SubscribeResponses. gNMIReverse is
// fire-and-forget: the only response is the Empty closing the RPC.
func (s *Server) Publish(stream grpc.ClientStreamingServer[gnmipb.SubscribeResponse, emptypb.Empty]) error {
	metrics.DialoutStreamOpened()
	defer metrics.DialoutStreamClosed()
	for {
		rsp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return err
		}
		s.handle(stream.Context(), rsp)
	}
}

func (s *Server) handle(ctx context.Context, rsp *gnmipb.SubscribeResponse) {
	notif := rsp.GetUpdate()
	if notif == nil {
		return
	}
	target := notif.GetPrefix().GetTarget()
	dev, ok := s.devices[target]
	if !ok {
		metrics.IncDialoutUnknownTarget()
		s.logUnknown(target)
		return
	}
	metrics.IncDialoutUpdateReceived(dev.entry.Name)

	dev.mu.Lock()
	records := dev.enricher.FromNotification(dev.entry.Address, notif)
	dev.mu.Unlock()
	for _, rec := range records {
		body, err := json.Marshal(rec)
		if err != nil {
			log.Printf("[%s] marshal: %v", dev.entry.Name, err)
			continue
		}
		s.sink.Send(ctx, dev.entry.Name, []byte(rec.Key), body)
	}
}

func (s *Server) logUnknown(target string) {
	s.unknownMu.Lock()
	defer s.unknownMu.Unlock()
	if time.Since(s.lastUnknownLog) < unknownLogInterval {
		return
	}
	s.lastUnknownLog = time.Now()
	log.Printf("dialout: dropping update from unknown target %q (not in dialout.devices; further drops counted, logged at most every %s)", target, unknownLogInterval)
}
