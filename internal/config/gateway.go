// SPDX-License-Identifier: Apache-2.0

package config

import "fmt"

// Gateway is the on-disk shape of configs/gateway.yaml.
type Gateway struct {
	Kafka Kafka    `yaml:"kafka"`
	GNMI  GNMI     `yaml:"gnmi"`
	Paths []string `yaml:"paths"`
	Hosts []string `yaml:"hosts"`
}

func LoadGateway(path string) (*Gateway, error) {
	var c Gateway
	if err := loadYAML(path, &c); err != nil {
		return nil, err
	}
	c.GNMI.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Gateway) validate() error {
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka.brokers is required")
	}
	if c.Kafka.Topic == "" {
		return fmt.Errorf("kafka.topic is required")
	}
	if len(c.Hosts) == 0 {
		return fmt.Errorf("hosts must have at least one entry")
	}
	if len(c.Paths) == 0 {
		return fmt.Errorf("paths must have at least one entry")
	}
	return nil
}
