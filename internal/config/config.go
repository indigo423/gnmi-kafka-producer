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
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Shared field types.

// Kafka configures the producer's connection to the brokers. Everything beyond
// brokers/topic is optional and additive: with no other fields set the producer
// connects plaintext without authentication, as it always has. SASL credentials
// follow the same env-var indirection as gNMI security profiles: names in YAML,
// presence verified at load, values read when the producer is built.
type Kafka struct {
	Brokers       []string `yaml:"brokers"`
	Topic         string   `yaml:"topic"`
	ClientID      string   `yaml:"client_id"`
	Compression   string   `yaml:"compression"`
	TLS           bool     `yaml:"tls"`
	TLSSkipVerify bool     `yaml:"tls_skip_verify"`
	SASLMechanism string   `yaml:"sasl_mechanism"`
	UsernameEnv   string   `yaml:"username_env"`
	PasswordEnv   string   `yaml:"password_env"`
}

// KafkaCompressions and KafkaSASLMechanisms are the allowed enum values
// (matched case-insensitively). The kafka package maps the same sets onto
// franz-go options; a test there pins the two together so they cannot drift.
var KafkaCompressions = map[string]bool{
	"none": true, "gzip": true, "snappy": true, "lz4": true, "zstd": true,
}

var KafkaSASLMechanisms = map[string]bool{
	"PLAIN": true, "SCRAM-SHA-256": true, "SCRAM-SHA-512": true,
}

func (k Kafka) validate() error {
	if len(k.Brokers) == 0 {
		return fmt.Errorf("kafka.brokers is required")
	}
	if k.Topic == "" {
		return fmt.Errorf("kafka.topic is required")
	}
	if k.Compression != "" && !KafkaCompressions[strings.ToLower(k.Compression)] {
		return fmt.Errorf("kafka.compression: unknown value %q (want none, gzip, snappy, lz4 or zstd)", k.Compression)
	}
	if k.TLSSkipVerify && !k.TLS {
		return fmt.Errorf("kafka.tls_skip_verify requires kafka.tls: true")
	}
	if (k.UsernameEnv == "") != (k.PasswordEnv == "") {
		return fmt.Errorf("kafka: username_env and password_env must be set together")
	}
	if k.SASLMechanism == "" {
		if k.UsernameEnv != "" {
			return fmt.Errorf("kafka: username_env/password_env require sasl_mechanism")
		}
		return nil
	}
	if !KafkaSASLMechanisms[strings.ToUpper(k.SASLMechanism)] {
		return fmt.Errorf("kafka.sasl_mechanism: unknown value %q (want PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512)", k.SASLMechanism)
	}
	if k.UsernameEnv == "" {
		return fmt.Errorf("kafka.sasl_mechanism requires username_env and password_env")
	}
	if !k.TLS {
		return fmt.Errorf("kafka.sasl_mechanism requires kafka.tls: true (credentials over plaintext are not allowed)")
	}
	return envCredentialPair("kafka", k.UsernameEnv, k.PasswordEnv)
}

// envCredentialPair enforces the shared credential rules: set both variable
// names or neither, and any referenced variable must be present and non-empty
// at load (fail fast; values are read at use time).
func envCredentialPair(scope, userEnv, passEnv string) error {
	if (userEnv == "") != (passEnv == "") {
		return fmt.Errorf("%s: username_env and password_env must be set together", scope)
	}
	if userEnv == "" {
		return nil
	}
	for _, env := range []string{userEnv, passEnv} {
		if v, ok := os.LookupEnv(env); !ok || v == "" {
			return fmt.Errorf("%s: environment variable %s is unset or empty", scope, env)
		}
	}
	return nil
}

// GNMI holds dial defaults shared by all targets. Authentication and transport
// security live in per-target SecurityProfiles.
type GNMI struct {
	Port        int           `yaml:"port"`
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

// SecurityProfile is one named block under security_profiles: the transport
// security and credentials for the gRPC channel to a target. TLS with verified
// certificates is the default; insecure (plaintext) and skip_verify are explicit
// opt-outs. Credentials are referenced by environment variable name — the config
// file never holds secret values. Presence is verified at load (fail fast); the
// values are read from the environment at dial time, so secrets never sit on
// the config struct.
type SecurityProfile struct {
	Insecure    bool   `yaml:"insecure"`
	SkipVerify  bool   `yaml:"skip_verify"`
	CACert      string `yaml:"ca_cert"`
	ClientCert  string `yaml:"client_cert"`
	ClientKey   string `yaml:"client_key"`
	UsernameEnv string `yaml:"username_env"`
	PasswordEnv string `yaml:"password_env"`
}

func (s SecurityProfile) validate(name string) error {
	if s.Insecure && (s.SkipVerify || s.CACert != "" || s.ClientCert != "" || s.ClientKey != "") {
		return fmt.Errorf("security_profiles.%s: insecure contradicts TLS fields (skip_verify, ca_cert, client_cert, client_key)", name)
	}
	if s.Insecure && s.UsernameEnv != "" {
		return fmt.Errorf("security_profiles.%s: credentials over an insecure (plaintext) channel are not allowed", name)
	}
	if s.SkipVerify && s.CACert != "" {
		return fmt.Errorf("security_profiles.%s: skip_verify and ca_cert are contradictory", name)
	}
	if (s.ClientCert == "") != (s.ClientKey == "") {
		return fmt.Errorf("security_profiles.%s: client_cert and client_key must be set together", name)
	}
	return envCredentialPair("security_profiles."+name, s.UsernameEnv, s.PasswordEnv)
}

// Target is one entry in the targets: registry. Address may be a bare host
// (port comes from gnmi.port) or host:port. Security references a
// security_profiles entry; Subscriptions references subscription_profiles
// entries. Labels are injected verbatim into every record for the target.
type Target struct {
	Name          string            `yaml:"name"`
	Address       string            `yaml:"address"`
	Security      string            `yaml:"security"`
	Labels        map[string]string `yaml:"labels"`
	Subscriptions []string          `yaml:"subscriptions"`
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
