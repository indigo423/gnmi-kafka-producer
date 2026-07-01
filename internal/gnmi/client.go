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

// BuildSubscribeRequest builds a STREAM SAMPLE SubscribeRequest covering every path,
// all sharing g.SampleInterval and g.Encoding.
func BuildSubscribeRequest(g config.GNMI, paths []string) (*gnmipb.SubscribeRequest, error) {
	interval := g.SampleInterval
	if interval == 0 {
		interval = 5 * time.Second
	}
	subs := make([]api.GNMIOption, 0, len(paths))
	for _, p := range paths {
		subs = append(subs, api.Subscription(
			api.Path(p),
			api.SubscriptionMode("sample"),
			api.SampleInterval(interval),
		))
	}
	reqOpts := []api.GNMIOption{
		api.Encoding(g.Encoding),
		api.SubscriptionListMode("stream"),
	}
	reqOpts = append(reqOpts, subs...)
	req, err := api.NewSubscribeRequest(reqOpts...)
	if err != nil {
		return nil, fmt.Errorf("build subscribe req: %w", err)
	}
	return req, nil
}

// SetAdminState issues a gNMI Set against the target to flip an interface's admin-state
// (plus its subinterface 0). Path/value format matches SR Linux native YANG.
func SetAdminState(ctx context.Context, tg *target.Target, iface, state string) error {
	req, err := api.NewSetRequest(
		api.Update(
			api.Path(fmt.Sprintf("/interface[name=%s]/admin-state", iface)),
			api.Value(state, "json_ietf"),
		),
		api.Update(
			api.Path(fmt.Sprintf("/interface[name=%s]/subinterface[index=0]/admin-state", iface)),
			api.Value(state, "json_ietf"),
		),
	)
	if err != nil {
		return fmt.Errorf("build set req: %w", err)
	}
	_, err = tg.Set(ctx, req)
	return err
}
