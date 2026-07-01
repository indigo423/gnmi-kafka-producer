// Package config holds YAML-backed config types for both the gateway and the driver.
//
// Each service has its own config file (and in k8s, its own ConfigMap) so
// they can be deployed and reconfigured independently. The shared field
// types (Kafka, GNMI, Flap) keep the YAML shape consistent across services.
package config

import (
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
	Port           int           `yaml:"port"`
	Username       string        `yaml:"username"`
	Password       string        `yaml:"password"`
	SkipVerify     bool          `yaml:"skip_verify"`
	Insecure       bool          `yaml:"insecure"`
	Encoding       string        `yaml:"encoding"`
	DialTimeout    time.Duration `yaml:"dial_timeout"`
	SampleInterval time.Duration `yaml:"sample_interval"`
}

type Flap struct {
	Enabled    bool          `yaml:"enabled"`
	Interval   time.Duration `yaml:"interval"`
	Interfaces []string      `yaml:"interfaces"`
}

func (g *GNMI) applyDefaults() {
	if g.Port == 0 {
		g.Port = 57400
	}
	if g.Encoding == "" {
		g.Encoding = "json_ietf"
	}
	if g.DialTimeout == 0 {
		g.DialTimeout = 10 * time.Second
	}
	if g.SampleInterval == 0 {
		g.SampleInterval = 5 * time.Second
	}
}

func (f *Flap) applyDefaults() {
	if f.Interval == 0 {
		f.Interval = 10 * time.Second
	}
}

// loadYAML reads and unmarshals a file into v.
func loadYAML(path string, v any) error {
	data, err := os.ReadFile(path) // #nosec G304 -- path is an operator-supplied -config flag, not untrusted input
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if err := yaml.Unmarshal(data, v); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	return nil
}
