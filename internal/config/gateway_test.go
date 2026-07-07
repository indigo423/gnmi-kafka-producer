// SPDX-License-Identifier: Apache-2.0

package config

import (
	"maps"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

// validGateway binds all profiles to one target, so single-target validation
// behaves like the old global checks.
func validGateway(profiles map[string]SubscriptionProfile) Gateway {
	return Gateway{
		Kafka:            Kafka{Brokers: []string{"kafka:9092"}, Topic: "gnmi.telemetry"},
		SecurityProfiles: map[string]SecurityProfile{"lab": {SkipVerify: true}},
		Profiles:         profiles,
		Targets: []Target{{
			Name:          "t1",
			Address:       "192.168.100.1",
			Security:      "lab",
			Subscriptions: slices.Sorted(maps.Keys(profiles)),
		}},
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
	cfg := validGateway(map[string]SubscriptionProfile{})
	cfg.Targets[0].Subscriptions = []string{"x"} // keep the target shape valid
	err := cfg.validate()
	checkErr(t, err, "subscription_profiles must have at least one entry", "")
}

func TestValidateSecurityProfiles(t *testing.T) {
	cases := []struct {
		name    string
		profile SecurityProfile
		wantErr string // substring; empty means valid
	}{
		{"default verified TLS", SecurityProfile{}, ""},
		{"skip_verify", SecurityProfile{SkipVerify: true}, ""},
		{"insecure", SecurityProfile{Insecure: true}, ""},
		{"mTLS", SecurityProfile{CACert: "ca.pem", ClientCert: "c.pem", ClientKey: "k.pem"}, ""},
		{"insecure with TLS field", SecurityProfile{Insecure: true, SkipVerify: true}, "insecure contradicts"},
		{"insecure with client cert", SecurityProfile{Insecure: true, ClientCert: "c.pem", ClientKey: "k.pem"}, "insecure contradicts"},
		{"insecure with credentials", SecurityProfile{Insecure: true, UsernameEnv: "U", PasswordEnv: "P"}, "credentials over an insecure"},
		{"skip_verify with ca_cert", SecurityProfile{SkipVerify: true, CACert: "ca.pem"}, "contradictory"},
		{"client_cert without key", SecurityProfile{ClientCert: "c.pem"}, "must be set together"},
		{"username_env without password_env", SecurityProfile{UsernameEnv: "U"}, "must be set together"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validGateway(map[string]SubscriptionProfile{"p": onChange("/a/b")})
			cfg.SecurityProfiles["lab"] = tc.profile
			checkErr(t, cfg.validate(), tc.wantErr, "lab")
		})
	}
}

func TestSecurityProfileEnvPresence(t *testing.T) {
	cfg := validGateway(map[string]SubscriptionProfile{"p": onChange("/a/b")})
	cfg.SecurityProfiles["lab"] = SecurityProfile{SkipVerify: true, UsernameEnv: "GW_TEST_USER", PasswordEnv: "GW_TEST_PASS"}

	// Unset variable fails fast at load, naming profile and variable. The
	// values themselves are read at dial time, not stored on the config.
	t.Setenv("GW_TEST_USER", "admin")
	checkErr(t, cfg.validate(), "GW_TEST_PASS is unset or empty", "lab")

	t.Setenv("GW_TEST_PASS", "s3cret")
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate() = %v, want nil", err)
	}
}

func TestValidateTargets(t *testing.T) {
	base := func() Gateway {
		return validGateway(map[string]SubscriptionProfile{"p": onChange("/a/b")})
	}
	cases := []struct {
		name    string
		mutate  func(*Gateway)
		wantErr string
	}{
		{"no targets and no dialout", func(g *Gateway) { g.Targets = nil }, "at least one of: dial-in targets or a dialout listener"},
		{"missing name", func(g *Gateway) { g.Targets[0].Name = "" }, "needs a name"},
		{"duplicate name", func(g *Gateway) { g.Targets = append(g.Targets, g.Targets[0]) }, "duplicate target name"},
		{"missing address", func(g *Gateway) { g.Targets[0].Address = "" }, "address is required"},
		{"unknown security ref", func(g *Gateway) { g.Targets[0].Security = "nope" }, `security profile "nope" is not defined`},
		{"empty subscriptions", func(g *Gateway) { g.Targets[0].Subscriptions = nil }, "subscriptions must have at least one entry"},
		{"unknown subscription ref", func(g *Gateway) { g.Targets[0].Subscriptions = []string{"nope"} }, `subscription profile "nope" is not defined`},
		{"duplicate subscription ref", func(g *Gateway) { g.Targets[0].Subscriptions = []string{"p", "p"} }, `duplicate subscription reference "p"`},
		{"reserved label key", func(g *Gateway) { g.Targets[0].Labels = map[string]string{"device": "x"} }, `label key "device" is reserved`},
		{"ordinary labels ok", func(g *Gateway) { g.Targets[0].Labels = map[string]string{"role": "leaf"} }, ""},
		{"duplicate address", func(g *Gateway) {
			g.Targets = append(g.Targets, Target{Name: "t2", Address: g.Targets[0].Address, Security: "lab", Subscriptions: []string{"p"}})
		}, `share address`},
		{"host:port address ok", func(g *Gateway) { g.Targets[0].Address = "192.168.100.1:57400" }, ""},
		{"bracketed IPv6 with port ok", func(g *Gateway) { g.Targets[0].Address = "[2001:db8::1]:9339" }, ""},
		{"bare IPv6 literal rejected", func(g *Gateway) { g.Targets[0].Address = "2001:db8::1" }, "not host:port"},
		{"URL address rejected", func(g *Gateway) { g.Targets[0].Address = "https://device:9339" }, "not a URL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base()
			tc.mutate(&cfg)
			checkErr(t, cfg.validate(), tc.wantErr, "")
		})
	}
}

func TestValidateKafkaTransport(t *testing.T) {
	cases := []struct {
		name    string
		kafka   Kafka
		wantErr string // substring; empty means valid
	}{
		{"plaintext default", Kafka{Brokers: []string{"k:9092"}, Topic: "t"}, ""},
		{"client_id and compression", Kafka{Brokers: []string{"k:9092"}, Topic: "t", ClientID: "gw", Compression: "snappy"}, ""},
		{"unknown compression", Kafka{Brokers: []string{"k:9092"}, Topic: "t", Compression: "fastest"}, `unknown value "fastest"`},
		{"compression case-insensitive", Kafka{Brokers: []string{"k:9092"}, Topic: "t", Compression: "Snappy"}, ""},
		{"tls alone", Kafka{Brokers: []string{"k:9092"}, Topic: "t", TLS: true}, ""},
		{"skip_verify without tls", Kafka{Brokers: []string{"k:9092"}, Topic: "t", TLSSkipVerify: true}, "tls_skip_verify requires"},
		{"sasl without creds", Kafka{Brokers: []string{"k:9092"}, Topic: "t", TLS: true, SASLMechanism: "PLAIN"}, "requires username_env and password_env"},
		{"sasl without tls", Kafka{Brokers: []string{"k:9092"}, Topic: "t", SASLMechanism: "SCRAM-SHA-512", UsernameEnv: "U", PasswordEnv: "P"}, "requires kafka.tls: true"},
		{"creds without sasl", Kafka{Brokers: []string{"k:9092"}, Topic: "t", UsernameEnv: "U", PasswordEnv: "P"}, "require sasl_mechanism"},
		{"one env only", Kafka{Brokers: []string{"k:9092"}, Topic: "t", UsernameEnv: "U"}, "must be set together"},
		{"unknown mechanism", Kafka{Brokers: []string{"k:9092"}, Topic: "t", TLS: true, SASLMechanism: "OAUTH", UsernameEnv: "U", PasswordEnv: "P"}, `unknown value "OAUTH"`},
		{"mechanism case-insensitive", Kafka{Brokers: []string{"k:9092"}, Topic: "t", SASLMechanism: "scram-sha-512", UsernameEnv: "U", PasswordEnv: "P"}, "requires kafka.tls: true"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validGateway(map[string]SubscriptionProfile{"p": onChange("/a/b")})
			cfg.Kafka = tc.kafka
			checkErr(t, cfg.validate(), tc.wantErr, "")
		})
	}
}

func TestValidateKafkaSASLEnvPresence(t *testing.T) {
	cfg := validGateway(map[string]SubscriptionProfile{"p": onChange("/a/b")})
	cfg.Kafka = Kafka{Brokers: []string{"k:9092"}, Topic: "t", TLS: true,
		SASLMechanism: "SCRAM-SHA-512", UsernameEnv: "KAFKA_TEST_USER", PasswordEnv: "KAFKA_TEST_PASS"}

	t.Setenv("KAFKA_TEST_USER", "svc")
	checkErr(t, cfg.validate(), "KAFKA_TEST_PASS is unset or empty", "")

	t.Setenv("KAFKA_TEST_PASS", "s3cret")
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate() = %v, want nil", err)
	}
}

func TestValidateMetricsPort(t *testing.T) {
	cfg := validGateway(map[string]SubscriptionProfile{"p": onChange("/a/b")})
	cfg.MetricsPort = 9090
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate() = %v, want nil", err)
	}
	cfg.MetricsPort = -1
	checkErr(t, cfg.validate(), "metrics_port must be a port number", "")
	cfg.MetricsPort = 70000
	checkErr(t, cfg.validate(), "metrics_port must be a port number", "")
}

func TestOverlapIsScopedPerTarget(t *testing.T) {
	// Profiles "a" and "b" duplicate the same path: legal while bound to
	// different targets, rejected when one target binds both.
	dup := "/interfaces/interface[name=*]/state/counters/in-octets"
	profiles := map[string]SubscriptionProfile{
		"a": sample(5*time.Second, dup),
		"b": sample(10*time.Second, dup),
	}
	cfg := validGateway(profiles)
	cfg.Targets = []Target{
		{Name: "t1", Address: "h1", Security: "lab", Subscriptions: []string{"a"}},
		{Name: "t2", Address: "h2", Security: "lab", Subscriptions: []string{"b"}},
	}
	if err := cfg.validate(); err != nil {
		t.Fatalf("disjoint bindings: validate() = %v, want nil", err)
	}

	cfg.Targets[0].Subscriptions = []string{"a", "b"}
	err := cfg.validate()
	checkErr(t, err, "duplicate path", "t1")
}

func TestUnboundProfileParseErrorStillFails(t *testing.T) {
	cfg := validGateway(map[string]SubscriptionProfile{
		"good": onChange("/a/b"),
		"bad":  onChange("/a/b[name=x"), // malformed, bound to no target
	})
	cfg.Targets[0].Subscriptions = []string{"good"}
	checkErr(t, cfg.validate(), "malformed", "bad")
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
	// heartbeat_intervel is a typo; unknown keys must fail strict decoding.
	cfg := `
kafka: {brokers: ["k:9092"], topic: t}
security_profiles:
  lab: {skip_verify: true}
subscription_profiles:
  status:
    mode: ON_CHANGE
    heartbeat_intervel: 5m
    paths: ["/interfaces/interface[name=*]/state/oper-status"]
targets:
  - {name: t1, address: h1, security: lab, subscriptions: [status]}
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

// validDialout returns a minimal correct dialout block for mutation in tests.
// Its device addresses differ from validGateway's target (192.168.100.1) so
// the two registries don't collide when a test enables both modes.
func validDialout() *Dialout {
	return &Dialout{
		Listen: ":57400",
		Devices: []DialoutDevice{
			{Name: "d1", Address: "192.168.200.1", Labels: map[string]string{"role": "leaf"}},
			{Name: "d2", Address: "192.168.200.2"},
		},
	}
}

func TestValidateDialout(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Dialout)
		wantErr string // substring; empty means valid
	}{
		{"valid plaintext", func(_ *Dialout) {}, ""},
		{"valid TLS pair", func(d *Dialout) {
			d.TLS = &DialoutTLS{CertFile: "/c.crt", KeyFile: "/c.key"}
		}, ""},
		{"missing listen", func(d *Dialout) { d.Listen = "" }, "listen is required"},
		{"malformed listen", func(d *Dialout) { d.Listen = "57400" }, "not host:port"},
		{"TLS missing key", func(d *Dialout) {
			d.TLS = &DialoutTLS{CertFile: "/c.crt"}
		}, "cert_file and key_file must be set together"},
		{"empty devices", func(d *Dialout) { d.Devices = nil }, "dialout.devices must have at least one entry"},
		{"device missing name", func(d *Dialout) { d.Devices[0].Name = "" }, "every device needs a name"},
		{"duplicate device name", func(d *Dialout) { d.Devices[1].Name = "d1" }, `duplicate device name "d1"`},
		{"device missing address", func(d *Dialout) { d.Devices[1].Address = "" }, "dialout.devices.d2: address is required"},
		{"duplicate device address", func(d *Dialout) { d.Devices[1].Address = "192.168.200.1" }, `share address "192.168.200.1"`},
		{"reserved label key", func(d *Dialout) {
			d.Devices[0].Labels = map[string]string{"device": "x"}
		}, `label key "device" is reserved`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validGateway(map[string]SubscriptionProfile{"p": onChange("/interfaces/interface[name=*]/state/oper-status")})
			cfg.Dialout = validDialout()
			tc.mutate(cfg.Dialout)
			checkErr(t, cfg.validate(), tc.wantErr, "")
		})
	}
}

func TestValidateModeCombinations(t *testing.T) {
	profiles := map[string]SubscriptionProfile{"p": onChange("/interfaces/interface[name=*]/state/oper-status")}

	t.Run("dial-in only stays valid", func(t *testing.T) {
		cfg := validGateway(profiles)
		checkErr(t, cfg.validate(), "", "")
	})
	t.Run("dial-out only: empty targets and no profiles is valid", func(t *testing.T) {
		cfg := validGateway(profiles)
		cfg.Targets = nil
		cfg.Profiles = nil
		cfg.Dialout = validDialout()
		checkErr(t, cfg.validate(), "", "")
	})
	t.Run("both modes together are valid", func(t *testing.T) {
		cfg := validGateway(profiles)
		cfg.Dialout = validDialout()
		checkErr(t, cfg.validate(), "", "")
	})
	t.Run("device sharing a target's address is rejected", func(t *testing.T) {
		cfg := validGateway(profiles) // target t1 @ 192.168.100.1
		cfg.Dialout = validDialout()
		cfg.Dialout.Devices[0].Address = "192.168.100.1"
		checkErr(t, cfg.validate(), "is also a dial-in target", "")
	})
	t.Run("device sharing a target's name is rejected", func(t *testing.T) {
		cfg := validGateway(profiles) // target t1
		cfg.Dialout = validDialout()
		cfg.Dialout.Devices[0].Name = "t1"
		checkErr(t, cfg.validate(), "is also a dial-in target", "")
	})
	t.Run("neither mode is rejected", func(t *testing.T) {
		cfg := validGateway(profiles)
		cfg.Targets = nil
		checkErr(t, cfg.validate(), "at least one of: dial-in targets or a dialout listener", "")
	})
}

func TestDialoutDeviceStaticFields(t *testing.T) {
	d := DialoutDevice{Name: "d1", Address: "192.168.100.1", Labels: map[string]string{"role": "leaf"}}
	got := d.StaticFields()
	if got["target"] != "d1" || got["role"] != "leaf" || len(got) != 2 {
		t.Fatalf("StaticFields() = %v, want target=d1 role=leaf", got)
	}
}
