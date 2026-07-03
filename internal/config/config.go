// Package config holds YAML-backed config types for the gateway.
//
// The gateway reads its config from a file (in k8s, a ConfigMap) so it can be
// reconfigured independently. The shared field types (Kafka, GNMI) keep the
// YAML shape consistent.
package config

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Shared field types.

type Kafka struct {
	Brokers []string `yaml:"brokers"`
	Topic   string   `yaml:"topic"`
}

type GNMI struct {
	Port        int           `yaml:"port"`
	Username    string        `yaml:"username"`
	Password    string        `yaml:"password"`
	SkipVerify  bool          `yaml:"skip_verify"`
	Insecure    bool          `yaml:"insecure"`
	Encoding    string        `yaml:"encoding"`
	DialTimeout time.Duration `yaml:"dial_timeout"`
}

func (g *GNMI) applyDefaults() {
	if g.Port == 0 {
		g.Port = 9339
	}
	if g.Encoding == "" {
		g.Encoding = "json_ietf"
	}
	if g.DialTimeout == 0 {
		g.DialTimeout = 10 * time.Second
	}
}

// SubscriptionProfile is one named block under subscription_profiles: a set of
// gNMI paths sharing a collection mode. SAMPLE profiles require sample_interval;
// ON_CHANGE profiles may set heartbeat_interval to force a resend when a leaf
// stays quiet. Fields that are meaningless for the mode are rejected, not
// ignored, so typos surface at load time.
type SubscriptionProfile struct {
	Mode              string        `yaml:"mode"`
	SampleInterval    time.Duration `yaml:"sample_interval"`
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	Paths             []string      `yaml:"paths"`
}

func (p SubscriptionProfile) validate(name string) error {
	switch p.Mode {
	case "SAMPLE":
		if p.SampleInterval <= 0 {
			return fmt.Errorf("subscription_profiles.%s: sample_interval is required for mode SAMPLE", name)
		}
		if p.HeartbeatInterval != 0 {
			return fmt.Errorf("subscription_profiles.%s: heartbeat_interval is not allowed for mode SAMPLE", name)
		}
	case "ON_CHANGE":
		if p.SampleInterval != 0 {
			return fmt.Errorf("subscription_profiles.%s: sample_interval is not allowed for mode ON_CHANGE", name)
		}
		if p.HeartbeatInterval < 0 {
			return fmt.Errorf("subscription_profiles.%s: heartbeat_interval must not be negative", name)
		}
	case "":
		return fmt.Errorf("subscription_profiles.%s: mode is required (SAMPLE or ON_CHANGE)", name)
	default:
		return fmt.Errorf("subscription_profiles.%s: unknown mode %q (want SAMPLE or ON_CHANGE)", name, p.Mode)
	}
	if len(p.Paths) == 0 {
		return fmt.Errorf("subscription_profiles.%s: paths must have at least one entry", name)
	}
	return nil
}

// loadYAML reads and unmarshals a file into v. Decoding is strict: unknown
// keys are an error, so typos and legacy config fields surface at load time
// instead of being silently ignored.
func loadYAML(path string, v any) error {
	data, err := os.ReadFile(path) // #nosec G304 -- path is an operator-supplied -config flag, not untrusted input
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	return nil
}
