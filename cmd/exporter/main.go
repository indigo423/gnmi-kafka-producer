// SPDX-License-Identifier: Apache-2.0

// The exporter bridges the gnmi.telemetry topic into Prometheus: it consumes
// the gateway's merged per-interface JSON records and serves their last-known
// state as gauges on /metrics (exporter pattern; Prometheus scrapes). It is
// part of the e2e demo stack, built only via docker compose, never released.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tbotnz/gnmi-kafka-producer/internal/exporter"
)

func main() {
	brokers := flag.String("brokers", "kafka:9092", "comma-separated Kafka bootstrap brokers")
	topic := flag.String("topic", "gnmi.telemetry", "telemetry topic to consume")
	listen := flag.String("listen", ":9108", "address to serve Prometheus metrics on")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	store := exporter.NewStore()
	reg := prometheus.NewRegistry()
	reg.MustRegister(store)

	// Replaying the topic from the start warms the state map after a restart;
	// records carry the full merged interface state, so replay converges on the
	// latest values (per-partition order) with no special casing.
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ClientID("gnmi-prom-exporter"),
		kgo.ConsumeTopics(*topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Fatalf("kafka: %v", err)
	}
	defer client.Close()

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	srv := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("serving metrics on %s/metrics", *listen)
		// The exporter's whole job is this endpoint; a bind failure is fatal.
		log.Fatalf("metrics: %v", srv.ListenAndServe())
	}()

	log.Printf("consuming %s from %s", *topic, *brokers)
	for ctx.Err() == nil {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			break
		}
		fetches.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.Canceled) {
				log.Printf("fetch %s/%d: %v", t, p, err)
			}
		})
		fetches.EachRecord(func(r *kgo.Record) {
			store.Update(r.Key, r.Value)
		})
	}
	log.Println("exporter stopped")
}
