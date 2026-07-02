// SPDX-License-Identifier: Apache-2.0

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

	log.Printf("gateway starting: hosts=%v paths=%d kafka=%v topic=%s",
		cfg.Hosts, len(cfg.Paths), cfg.Kafka.Brokers, cfg.Kafka.Topic)

	var wg sync.WaitGroup
	for _, host := range cfg.Hosts {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			runHost(ctx, host, cfg.GNMI, cfg.Paths, producer)
		}(host)
	}
	wg.Wait()
	log.Println("gateway stopped")
}

func runHost(ctx context.Context, host string, g config.GNMI, paths []string, producer *kafka.Producer) {
	log.Printf("[%s] connecting (paths=%d)", host, len(paths))
	tg, err := gnmix.Dial(ctx, host, g)
	if err != nil {
		log.Printf("[%s] dial gave up: %v", host, err)
		return
	}
	defer func() { _ = tg.Close() }()

	req, err := gnmix.BuildSubscribeRequest(g, paths)
	if err != nil {
		log.Printf("[%s] build subscribe: %v", host, err)
		return
	}

	go tg.Subscribe(ctx, req, "main")
	rspCh, errCh := tg.ReadSubscriptions()

	log.Printf("[%s] subscribed", host)
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
			for _, rec := range gnmix.FromNotification(host, notif) {
				body, err := json.Marshal(rec)
				if err != nil {
					log.Printf("[%s] marshal: %v", host, err)
					continue
				}
				producer.Send(ctx, []byte(rec.Path), body)
			}
		}
	}
}
