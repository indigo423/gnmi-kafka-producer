package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os/signal"
	"sync"
	"syscall"

	"github.com/tbotnz/gnmi-kafka-producer/internal/config"
	gnmix "github.com/tbotnz/gnmi-kafka-producer/internal/gnmi"
	"github.com/tbotnz/gnmi-kafka-producer/internal/kafka"
	"github.com/tbotnz/gnmi-kafka-producer/internal/metrics"
)

func main() {
	cfgPath := flag.String("config", "/etc/gnmi-kafka/gateway.yaml", "path to gateway config YAML")
	flag.Parse()

	cfg, err := config.LoadGateway(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	producer, err := kafka.NewProducer(cfg.Kafka)
	if err != nil {
		log.Fatalf("kafka: %v", err)
	}
	defer producer.Close(context.Background())

	log.Printf("gateway starting: targets=%d profiles=%d kafka=%v topic=%s",
		len(cfg.Targets), len(cfg.Profiles), cfg.Kafka.Brokers, cfg.Kafka.Topic)

	if cfg.MetricsPort > 0 {
		go func() {
			log.Printf("metrics: serving http://:%d/metrics", cfg.MetricsPort)
			// The operator asked for metrics; running without them would be a
			// silent loss of the health signal, so a bind failure is fatal.
			log.Fatalf("metrics: %v", metrics.Serve(cfg.MetricsPort))
		}()
	}

	var wg sync.WaitGroup
	for _, t := range cfg.Targets {
		wg.Add(1)
		go func(t config.Target) {
			defer wg.Done()
			runTarget(ctx, t, cfg, producer)
		}(t)
	}
	wg.Wait()
	log.Println("gateway stopped")
}

func runTarget(ctx context.Context, t config.Target, cfg *config.Gateway, producer *kafka.Producer) {
	// References are validated at config load, so these lookups always hit.
	sec := cfg.SecurityProfiles[t.Security]
	profiles := make(map[string]config.SubscriptionProfile, len(t.Subscriptions))
	for _, name := range t.Subscriptions {
		profiles[name] = cfg.Profiles[name]
	}

	log.Printf("[%s] connecting to %s (profiles=%d)", t.Name, t.Address, len(profiles))
	tg, err := gnmix.Dial(ctx, t, sec, cfg.GNMI)
	if err != nil {
		log.Printf("[%s] dial gave up: %v", t.Name, err)
		return
	}
	defer func() { _ = tg.Close() }()

	reqs, err := gnmix.BuildSubscribeRequests(cfg.GNMI, profiles)
	if err != nil {
		log.Printf("[%s] build subscribe: %v", t.Name, err)
		return
	}

	// One Subscribe RPC per profile (targets may reject mixed-mode lists);
	// ReadSubscriptions fans them all into one response stream. Health gauges
	// start at 0 and flip on attributed updates/errors below.
	for name, req := range reqs {
		metrics.SetSubscriptionUp(t.Name, name, false)
		go tg.Subscribe(ctx, req, name)
	}
	rspCh, errCh := tg.ReadSubscriptions()

	// Each target owns its Enricher, so rate state needs no locking. Labels and
	// the target name ride on every record as static fields.
	enricher := gnmix.NewEnricher(t.StaticFields())

	log.Printf("[%s] subscriptions started (profiles=%d)", t.Name, len(reqs))
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-errCh:
			if e != nil {
				metrics.IncSubscribeError(t.Name, e.SubscriptionName)
				metrics.SetSubscriptionUp(t.Name, e.SubscriptionName, false)
				log.Printf("[%s] subscribe error (%s): %v", t.Name, e.SubscriptionName, e.Err)
			}
		case rsp, ok := <-rspCh:
			if !ok {
				return
			}
			metrics.SetSubscriptionUp(t.Name, rsp.SubscriptionName, true)
			notif := rsp.Response.GetUpdate()
			if notif == nil {
				continue
			}
			for _, rec := range enricher.FromNotification(t.Address, notif) {
				body, err := json.Marshal(rec)
				if err != nil {
					log.Printf("[%s] marshal: %v", t.Name, err)
					continue
				}
				producer.Send(ctx, t.Name, []byte(rec.Key), body)
			}
		}
	}
}
