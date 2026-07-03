// SPDX-License-Identifier: Apache-2.0

package gnmi

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	"github.com/openconfig/gnmic/pkg/api"
	"github.com/openconfig/gnmic/pkg/api/target"
	"github.com/tbotnz/gnmi-kafka-producer/internal/config"
)

// endpoint returns "host:port", respecting host strings that already contain a port.
func endpoint(host string, defaultPort int) string {
	if strings.Contains(host, ":") {
		return host
	}
	return fmt.Sprintf("%s:%d", host, defaultPort)
}

// Dial creates a gnmic Target and opens the gRPC channel, retrying until ctx is cancelled.
func Dial(ctx context.Context, host string, g config.GNMI) (*target.Target, error) {
	opts := []api.TargetOption{
		api.Name(host),
		api.Address(endpoint(host, g.Port)),
		api.Username(g.Username),
		api.Password(g.Password),
		api.Timeout(g.DialTimeout),
	}
	if g.Insecure {
		opts = append(opts, api.Insecure(true))
	} else {
		opts = append(opts, api.SkipVerify(g.SkipVerify))
	}

	var lastErr error
	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		tg, err := api.NewTarget(opts...)
		if err != nil {
			lastErr = err
		} else if err := tg.CreateGNMIClient(ctx); err == nil {
			// gnmic's Subscribe retries a failed stream after RetryTimer; the
			// api package exposes no option for it and the zero default
			// busy-loops ("retrying in 0s") on a persistent rejection.
			tg.Config.RetryTimer = 5 * time.Second
			return tg, nil
		} else {
			lastErr = err
			_ = tg.Close()
		}
		log.Printf("[%s] dial attempt %d failed: %v", host, attempt, lastErr)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

// BuildSubscribeRequests builds one STREAM SubscribeRequest per profile, each
// subscription entry carrying its profile's mode, sample interval, and heartbeat
// interval. Requests are per profile because targets may reject a
// SubscriptionList that mixes ON_CHANGE and SAMPLE modes (nl6 does). All of a
// host's requests are subscribed on its one connection and fanned into one
// response stream, so the host's single Enricher and the merged-record contract
// are preserved.
func BuildSubscribeRequests(g config.GNMI, profiles map[string]config.SubscriptionProfile) (map[string]*gnmipb.SubscribeRequest, error) {
	reqs := make(map[string]*gnmipb.SubscribeRequest, len(profiles))
	for name, p := range profiles {
		reqOpts := []api.GNMIOption{
			api.Encoding(g.Encoding),
			api.SubscriptionListMode("stream"),
		}
		for _, path := range p.Paths {
			subOpts := []api.GNMIOption{
				api.Path(path),
				api.SubscriptionMode(p.Mode),
			}
			switch p.Mode {
			case "SAMPLE":
				subOpts = append(subOpts, api.SampleInterval(p.SampleInterval))
			case "ON_CHANGE":
				if p.HeartbeatInterval > 0 {
					subOpts = append(subOpts, api.HeartbeatInterval(p.HeartbeatInterval))
				}
			}
			reqOpts = append(reqOpts, api.Subscription(subOpts...))
		}
		req, err := api.NewSubscribeRequest(reqOpts...)
		if err != nil {
			return nil, fmt.Errorf("build subscribe req for profile %s: %w", name, err)
		}
		reqs[name] = req
	}
	return reqs, nil
}
