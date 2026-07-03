package config

import (
	"fmt"
	"sort"
)

// Gateway is the on-disk shape of configs/gateway.yaml.
type Gateway struct {
	Kafka    Kafka                          `yaml:"kafka"`
	GNMI     GNMI                           `yaml:"gnmi"`
	Profiles map[string]SubscriptionProfile `yaml:"subscription_profiles"`
	Hosts    []string                       `yaml:"hosts"`
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
	if len(c.Profiles) == 0 {
		return fmt.Errorf("subscription_profiles must have at least one entry")
	}
	names := make([]string, 0, len(c.Profiles))
	for name := range c.Profiles {
		names = append(names, name)
	}
	sort.Strings(names) // deterministic error output when several profiles are invalid
	for _, name := range names {
		if err := c.Profiles[name].validate(name); err != nil {
			return err
		}
	}
	return validateNoOverlap(c.Profiles)
}
