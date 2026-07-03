// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func validGateway(profiles map[string]SubscriptionProfile) Gateway {
	return Gateway{
		Kafka:    Kafka{Brokers: []string{"kafka:9092"}, Topic: "gnmi.telemetry"},
		Profiles: profiles,
		Hosts:    []string{"192.168.100.1"},
	}
}

func sample(interval time.Duration, paths ...string) SubscriptionProfile {
	return SubscriptionProfile{Mode: "SAMPLE", SampleInterval: interval, Paths: paths}
}

func onChange(paths ...string) SubscriptionProfile {
	return SubscriptionProfile{Mode: "ON_CHANGE", Paths: paths}
}

func TestValidateProfileFieldRules(t *testing.T) {
	cases := []struct {
		name    string
		profile SubscriptionProfile
		wantErr string // substring; empty means valid
	}{
		{"valid SAMPLE", sample(5*time.Second, "/interfaces/interface[name=*]/state/counters/in-octets"), ""},
		{"valid ON_CHANGE with heartbeat", SubscriptionProfile{
			Mode: "ON_CHANGE", HeartbeatInterval: 5 * time.Minute,
			Paths: []string{"/interfaces/interface[name=*]/state/oper-status"},
		}, ""},
		{"SAMPLE missing sample_interval", SubscriptionProfile{
			Mode: "SAMPLE", Paths: []string{"/a/b"},
		}, "sample_interval is required"},
		{"SAMPLE with heartbeat_interval", SubscriptionProfile{
			Mode: "SAMPLE", SampleInterval: time.Second, HeartbeatInterval: time.Minute, Paths: []string{"/a/b"},
		}, "heartbeat_interval is not allowed"},
		{"ON_CHANGE with sample_interval", SubscriptionProfile{
			Mode: "ON_CHANGE", SampleInterval: time.Second, Paths: []string{"/a/b"},
		}, "sample_interval is not allowed"},
		{"ON_CHANGE with negative heartbeat_interval", SubscriptionProfile{
			Mode: "ON_CHANGE", HeartbeatInterval: -5 * time.Minute, Paths: []string{"/a/b"},
		}, "heartbeat_interval must not be negative"},
		{"missing mode", SubscriptionProfile{Paths: []string{"/a/b"}}, "mode is required"},
		{"unknown mode", SubscriptionProfile{Mode: "PERIODIC", Paths: []string{"/a/b"}}, `unknown mode "PERIODIC"`},
		{"empty paths", SubscriptionProfile{Mode: "ON_CHANGE"}, "paths must have at least one entry"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validGateway(map[string]SubscriptionProfile{"p": tc.profile})
			err := cfg.validate()
			checkErr(t, err, tc.wantErr, "p")
		})
	}
}

func TestValidateRequiresProfiles(t *testing.T) {
	cfg := validGateway(nil)
	err := cfg.validate()
	checkErr(t, err, "subscription_profiles must have at least one entry", "")
}

func TestValidateOverlap(t *testing.T) {
	cases := []struct {
		name    string
		a, b    SubscriptionProfile
		wantErr string // substring; empty means valid
	}{
		{
			"duplicate path across profiles",
			sample(5*time.Second, "/interfaces/interface[name=*]/state/counters/in-octets"),
			sample(10*time.Second, "/interfaces/interface[name=*]/state/counters/in-octets"),
			"duplicate path",
		},
		{
			"parent container subsumes child leaf",
			sample(5*time.Second, "/interfaces/interface[name=*]/state"),
			sample(10*time.Second, "/interfaces/interface[name=*]/state/counters/in-octets"),
			"subsumes",
		},
		{
			"wildcard key subsumes specific key",
			sample(5*time.Second, "/interfaces/interface[name=*]/state/counters"),
			sample(10*time.Second, "/interfaces/interface[name=eth0]/state/counters/in-octets"),
			"subsumes",
		},
		{
			"absent key subsumes keyed child",
			sample(5*time.Second, "/interfaces/interface/state/counters"),
			sample(10*time.Second, "/interfaces/interface[name=eth0]/state/counters/in-octets"),
			"subsumes",
		},
		{
			"disjoint sibling leaves accepted",
			sample(5*time.Second, "/interfaces/interface[name=*]/state/counters/in-octets"),
			onChange("/interfaces/interface[name=*]/state/oper-status"),
			"",
		},
		{
			"same leaf different specific keys accepted",
			sample(5*time.Second, "/interfaces/interface[name=eth0]/state/counters/in-octets"),
			sample(10*time.Second, "/interfaces/interface[name=eth1]/state/counters/in-octets"),
			"",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validGateway(map[string]SubscriptionProfile{"a": tc.a, "b": tc.b})
			err := cfg.validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validate() = %v, want nil", err)
				}
				return
			}
			// Overlap errors must name both profiles and both paths.
			checkErr(t, err, tc.wantErr, "a")
			checkErr(t, err, tc.wantErr, "b")
			for _, p := range append(tc.a.Paths, tc.b.Paths...) {
				if !strings.Contains(err.Error(), p) {
					t.Errorf("error %q does not name path %q", err, p)
				}
			}
		})
	}
}

func TestValidateRejectsMalformedPath(t *testing.T) {
	// Semantics follow gnmic's path.ParsePath — the same parser the subscribe
	// builder uses — so anything rejected here would also fail at subscribe time.
	for _, bad := range []string{"", "/a/b[name=x", "/a/b]", "/a/b[noequals]", "/a/b[name=]"} {
		cfg := validGateway(map[string]SubscriptionProfile{"p": onChange(bad)})
		if err := cfg.validate(); err == nil {
			t.Errorf("validate() accepted malformed path %q", bad)
		}
	}
}

func TestLoadGatewayRejectsUnknownKeys(t *testing.T) {
	dir := t.TempDir()
	file := dir + "/gateway.yaml"
	// heartbeat_intervel is a typo; legacy top-level paths is a removed field.
	cfg := `
kafka: {brokers: ["k:9092"], topic: t}
subscription_profiles:
  status:
    mode: ON_CHANGE
    heartbeat_intervel: 5m
    paths: ["/interfaces/interface[name=*]/state/oper-status"]
hosts: [h1]
`
	if err := os.WriteFile(file, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadGateway(file); err == nil || !strings.Contains(err.Error(), "heartbeat_intervel") {
		t.Fatalf("LoadGateway() = %v, want unknown-field error naming heartbeat_intervel", err)
	}
}

// checkErr asserts err is non-nil (when want != "") and mentions both the
// wanted substring and the profile name.
func checkErr(t *testing.T, err error, want, profile string) {
	t.Helper()
	if want == "" {
		if err != nil {
			t.Fatalf("validate() = %v, want nil", err)
		}
		return
	}
	if err == nil {
		t.Fatalf("validate() = nil, want error containing %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("validate() = %q, want substring %q", err, want)
	}
	if profile != "" && !strings.Contains(err.Error(), profile) {
		t.Fatalf("error %q does not name profile %q", err, profile)
	}
}
