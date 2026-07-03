package config

import (
	"fmt"
	"maps"
	"net"
	"slices"
	"strings"
)

// Gateway is the on-disk shape of configs/gateway.yaml.
type Gateway struct {
	Kafka            Kafka                          `yaml:"kafka"`
	GNMI             GNMI                           `yaml:"gnmi"`
	SecurityProfiles map[string]SecurityProfile     `yaml:"security_profiles"`
	Profiles         map[string]SubscriptionProfile `yaml:"subscription_profiles"`
	Targets          []Target                       `yaml:"targets"`
}

// reservedLabelKeys are record fields a target label must not shadow: the
// fields the enricher writes on every record plus the "target" key
// StaticFields injects.
var reservedLabelKeys = map[string]bool{
	"device": true, "interface": true, "timestamp": true, "target": true,
}

// StaticFields returns the per-target constant record fields: the labels
// verbatim plus "target" carrying the target name.
func (t Target) StaticFields() map[string]any {
	fields := make(map[string]any, len(t.Labels)+1)
	for k, v := range t.Labels {
		fields[k] = v
	}
	fields["target"] = t.Name
	return fields
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
	if len(c.Targets) == 0 {
		return fmt.Errorf("targets must have at least one entry")
	}
	if len(c.Profiles) == 0 {
		return fmt.Errorf("subscription_profiles must have at least one entry")
	}
	for _, name := range slices.Sorted(maps.Keys(c.Profiles)) {
		if err := c.Profiles[name].validate(name); err != nil {
			return err
		}
	}
	for _, name := range slices.Sorted(maps.Keys(c.SecurityProfiles)) {
		if err := c.SecurityProfiles[name].validate(name); err != nil {
			return err
		}
	}
	if err := c.validateTargets(); err != nil {
		return err
	}
	// Oversubscription is a per-device condition: check each target's
	// bound-profile union. Paths are parsed once up front so a parse error in
	// an unbound profile still fails the load.
	parsed, err := parseProfilePaths(c.Profiles)
	if err != nil {
		return err
	}
	for _, t := range c.Targets {
		if err := validateNoOverlap(t.Name, t.Subscriptions, parsed); err != nil {
			return err
		}
	}
	return nil
}

func (c *Gateway) validateTargets() error {
	seenName := make(map[string]bool, len(c.Targets))
	seenAddr := make(map[string]string, len(c.Targets))
	for _, t := range c.Targets {
		if t.Name == "" {
			return fmt.Errorf("targets: every target needs a name")
		}
		if seenName[t.Name] {
			return fmt.Errorf("targets: duplicate target name %q", t.Name)
		}
		seenName[t.Name] = true
		if err := validateAddress(t.Name, t.Address); err != nil {
			return err
		}
		// A shared address means two Enrichers emit records under one Kafka
		// key, interleaving their rate baselines into garbage bps values.
		if other, dup := seenAddr[t.Address]; dup {
			return fmt.Errorf("targets: %s and %s share address %q", other, t.Name, t.Address)
		}
		seenAddr[t.Address] = t.Name
		if _, ok := c.SecurityProfiles[t.Security]; !ok {
			return fmt.Errorf("targets.%s: security profile %q is not defined in security_profiles", t.Name, t.Security)
		}
		if len(t.Subscriptions) == 0 {
			return fmt.Errorf("targets.%s: subscriptions must have at least one entry", t.Name)
		}
		seenSub := make(map[string]bool, len(t.Subscriptions))
		for _, sub := range t.Subscriptions {
			if _, ok := c.Profiles[sub]; !ok {
				return fmt.Errorf("targets.%s: subscription profile %q is not defined in subscription_profiles", t.Name, sub)
			}
			if seenSub[sub] {
				return fmt.Errorf("targets.%s: duplicate subscription reference %q", t.Name, sub)
			}
			seenSub[sub] = true
		}
		for key := range t.Labels {
			if reservedLabelKeys[key] {
				return fmt.Errorf("targets.%s: label key %q is reserved (record field)", t.Name, key)
			}
		}
	}
	return nil
}

// validateAddress accepts a bare host (default port applies) or host:port.
// Anything the dialer would misparse — a URL scheme, or a bare IPv6 literal
// whose colons look like a port separator — is rejected at load.
func validateAddress(target, addr string) error {
	if addr == "" {
		return fmt.Errorf("targets.%s: address is required", target)
	}
	if strings.Contains(addr, "://") {
		return fmt.Errorf("targets.%s: address %q must be host or host:port, not a URL", target, addr)
	}
	if strings.Contains(addr, ":") {
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return fmt.Errorf("targets.%s: address %q is not host:port (write IPv6 literals as [addr]:port): %w", target, addr, err)
		}
	}
	return nil
}
