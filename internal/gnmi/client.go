package gnmi

import (
	"context"
	"fmt"
	"log"
	"os"
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

// Dial creates a gnmic Target for t using its security profile and opens the
// gRPC channel, retrying until ctx is cancelled. Credentials are read from the
// environment here (presence was verified at config load), so secrets never
// live on the config structs.
func Dial(ctx context.Context, t config.Target, sec config.SecurityProfile, g config.GNMI) (*target.Target, error) {
	addr := endpoint(t.Address, g.Port)
	opts := []api.TargetOption{
		api.Name(t.Name),
		api.Address(addr),
		api.Timeout(g.DialTimeout),
	}
	if sec.UsernameEnv != "" {
		opts = append(opts, api.Username(os.Getenv(sec.UsernameEnv)), api.Password(os.Getenv(sec.PasswordEnv)))
	}
	if sec.Insecure {
		opts = append(opts, api.Insecure(true))
	} else {
		if sec.SkipVerify {
			opts = append(opts, api.SkipVerify(true))
		}
		if sec.CACert != "" {
			opts = append(opts, api.TLSCA(sec.CACert))
		}
		if sec.ClientCert != "" {
			opts = append(opts, api.TLSCert(sec.ClientCert), api.TLSKey(sec.ClientKey))
		}
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
		log.Printf("[%s] dial attempt %d to %s failed: %v", t.Name, attempt, addr, lastErr)
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
// target's requests are subscribed on its one connection and fanned into one
// response stream, so the target's single Enricher and the merged-record
// contract are preserved.
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
