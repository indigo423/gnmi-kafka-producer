package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/openconfig/gnmic/pkg/api/target"
	"github.com/tbotnz/gnmi-kafka-producer/internal/config"
	gnmix "github.com/tbotnz/gnmi-kafka-producer/internal/gnmi"
)

func main() {
	cfgPath := flag.String("config", "/etc/gnmi-kafka/driver.yaml", "path to driver config YAML")
	flag.Parse()

	cfg, err := config.LoadDriver(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	if !cfg.Flap.Enabled {
		log.Println("flap.enabled=false, exiting")
		return
	}
	if len(cfg.Flap.Interfaces) == 0 {
		log.Println("flap.interfaces is empty, exiting")
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	log.Printf("driver starting: hosts=%v interfaces=%v interval=%s",
		cfg.Hosts, cfg.Flap.Interfaces, cfg.Flap.Interval)

	var wg sync.WaitGroup
	for _, host := range cfg.Hosts {
		wg.Add(1)
		go func(host string) {
			defer wg.Done()
			runHost(ctx, host, cfg)
		}(host)
	}
	wg.Wait()
	log.Println("driver stopped")
}

func runHost(ctx context.Context, host string, cfg *config.Driver) {
	tg, err := gnmix.Dial(ctx, host, cfg.GNMI)
	if err != nil {
		log.Printf("[%s] dial gave up: %v", host, err)
		return
	}
	defer func() { _ = tg.Close() }()

	var wg sync.WaitGroup
	for _, iface := range cfg.Flap.Interfaces {
		wg.Add(1)
		go func(iface string) {
			defer wg.Done()
			flap(ctx, host, tg, iface, cfg.Flap.Interval)
		}(iface)
	}
	wg.Wait()
}

func flap(ctx context.Context, host string, tg *target.Target, iface string, interval time.Duration) {
	for {
		for _, state := range []string{"enable", "disable"} {
			if err := gnmix.SetAdminState(ctx, tg, iface, state); err != nil {
				log.Printf("[%s] set %s admin-state=%s: %v", host, iface, state, err)
			} else {
				log.Printf("[%s] set %s admin-state=%s", host, iface, state)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(interval):
			}
		}
	}
}
