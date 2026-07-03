// SPDX-License-Identifier: Apache-2.0

package gnmi

import (
	"testing"
	"time"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	"github.com/tbotnz/gnmi-kafka-producer/internal/config"
)

// protoDur converts a proto nanosecond interval for comparison.
func protoDur(ns uint64) time.Duration {
	return time.Duration(ns) // #nosec G115 -- test intervals are far below int64 max
}

func TestBuildSubscribeRequestsMixedProfiles(t *testing.T) {
	g := config.GNMI{Encoding: "json_ietf"}
	profiles := map[string]config.SubscriptionProfile{
		"interface-counters": {
			Mode:           "SAMPLE",
			SampleInterval: 10 * time.Second,
			Paths: []string{
				"/interfaces/interface[name=*]/state/counters/in-octets",
				"/interfaces/interface[name=*]/state/counters/out-octets",
			},
		},
		"interface-status": {
			Mode:              "ON_CHANGE",
			HeartbeatInterval: 5 * time.Minute,
			Paths:             []string{"/interfaces/interface[name=*]/state/oper-status"},
		},
	}

	reqs, err := BuildSubscribeRequests(g, profiles)
	if err != nil {
		t.Fatalf("BuildSubscribeRequests: %v", err)
	}
	// One request per profile: targets may reject mixed-mode subscription lists.
	if len(reqs) != 2 {
		t.Fatalf("got %d requests, want 2 (one per profile)", len(reqs))
	}

	counters := subscriptionList(t, reqs, "interface-counters")
	if n := len(counters.GetSubscription()); n != 2 {
		t.Fatalf("interface-counters: got %d subscriptions, want 2", n)
	}
	for i, s := range counters.GetSubscription() {
		if s.GetMode() != gnmipb.SubscriptionMode_SAMPLE {
			t.Errorf("counters sub[%d] mode = %v, want SAMPLE", i, s.GetMode())
		}
		if got := protoDur(s.GetSampleInterval()); got != 10*time.Second {
			t.Errorf("counters sub[%d] sample_interval = %v, want 10s", i, got)
		}
		if s.GetHeartbeatInterval() != 0 {
			t.Errorf("counters sub[%d] has heartbeat_interval, want none", i)
		}
	}

	status := subscriptionList(t, reqs, "interface-status")
	if n := len(status.GetSubscription()); n != 1 {
		t.Fatalf("interface-status: got %d subscriptions, want 1", n)
	}
	s := status.GetSubscription()[0]
	if s.GetMode() != gnmipb.SubscriptionMode_ON_CHANGE {
		t.Errorf("status sub mode = %v, want ON_CHANGE", s.GetMode())
	}
	if got := protoDur(s.GetHeartbeatInterval()); got != 5*time.Minute {
		t.Errorf("status sub heartbeat_interval = %v, want 5m", got)
	}
	if s.GetSampleInterval() != 0 {
		t.Errorf("status sub has sample_interval, want none")
	}
	elems := s.GetPath().GetElem()
	if leaf := elems[len(elems)-1].GetName(); leaf != "oper-status" {
		t.Errorf("status sub leaf = %q, want %q", leaf, "oper-status")
	}
}

// subscriptionList asserts the named request exists and is a STREAM JSON_IETF
// SubscriptionList, returning it.
func subscriptionList(t *testing.T, reqs map[string]*gnmipb.SubscribeRequest, name string) *gnmipb.SubscriptionList {
	t.Helper()
	req, ok := reqs[name]
	if !ok {
		t.Fatalf("no request for profile %q", name)
	}
	sl := req.GetSubscribe()
	if sl == nil {
		t.Fatalf("profile %q: request has no SubscriptionList", name)
	}
	if sl.GetMode() != gnmipb.SubscriptionList_STREAM {
		t.Errorf("profile %q: list mode = %v, want STREAM", name, sl.GetMode())
	}
	if sl.GetEncoding() != gnmipb.Encoding_JSON_IETF {
		t.Errorf("profile %q: encoding = %v, want JSON_IETF", name, sl.GetEncoding())
	}
	return sl
}
