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

	log.Printf("gateway starting: hosts=%v profiles=%d kafka=%v topic=%s",
		cfg.Hosts, len(cfg.Profiles), cfg.Kafka.Brokers, cfg.Kafka.Topic)

	var wg sync.WaitGroup
	for _, host := range cfg.Hosts {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			runHost(ctx, host, cfg.GNMI, cfg.Profiles, producer)
		}(host)
	}
	wg.Wait()
	log.Println("gateway stopped")
}

func runHost(ctx context.Context, host string, g config.GNMI, profiles map[string]config.SubscriptionProfile, producer *kafka.Producer) {
	log.Printf("[%s] connecting (profiles=%d)", host, len(profiles))
	tg, err := gnmix.Dial(ctx, host, g)
	if err != nil {
		log.Printf("[%s] dial gave up: %v", host, err)
		return
	}
	defer func() { _ = tg.Close() }()

	reqs, err := gnmix.BuildSubscribeRequests(g, profiles)
	if err != nil {
		log.Printf("[%s] build subscribe: %v", host, err)
		return
	}

	// One Subscribe RPC per profile (targets may reject mixed-mode lists);
	// ReadSubscriptions fans them all into one response stream.
	for name, req := range reqs {
		go tg.Subscribe(ctx, req, name)
	}
	rspCh, errCh := tg.ReadSubscriptions()

	// Each host owns its Enricher, so rate state needs no locking.
	enricher := gnmix.NewEnricher()

	log.Printf("[%s] subscriptions started (profiles=%d)", host, len(reqs))
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-errCh:
			if e != nil {
				log.Printf("[%s] subscribe error: %v", host, e.Err)
			}
		case rsp, ok := <-rspCh:
			if !ok {
				return
			}
			notif := rsp.Response.GetUpdate()
			if notif == nil {
				continue
			}
			for _, rec := range enricher.FromNotification(host, notif) {
				body, err := json.Marshal(rec)
				if err != nil {
					log.Printf("[%s] marshal: %v", host, err)
					continue
				}
				producer.Send(ctx, []byte(rec.Key), body)
			}
		}
	}
}
