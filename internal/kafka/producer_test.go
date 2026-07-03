// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"testing"

	"github.com/tbotnz/gnmi-kafka-producer/internal/config"
	"github.com/twmb/franz-go/pkg/kgo"
)

// kgo.NewClient validates its options without dialing, so each config variant
// can be checked to build a client (or fail) without a broker.
func TestClientOptsVariants(t *testing.T) {
	t.Setenv("KAFKA_TEST_USER", "svc")
	t.Setenv("KAFKA_TEST_PASS", "s3cret")

	base := config.Kafka{Brokers: []string{"broker:9092"}, Topic: "t"}
	sasl := base
	sasl.TLS = true
	sasl.SASLMechanism = "SCRAM-SHA-512"
	sasl.UsernameEnv = "KAFKA_TEST_USER"
	sasl.PasswordEnv = "KAFKA_TEST_PASS"

	hardened := base
	hardened.ClientID = "gnmi-gateway"
	hardened.Compression = "snappy"
	hardened.TLS = true
	hardened.TLSSkipVerify = true

	for name, cfg := range map[string]config.Kafka{
		"plaintext default": base,
		"tls with options":  hardened,
		"sasl scram":        sasl,
	} {
		t.Run(name, func(t *testing.T) {
			opts, err := clientOpts(cfg)
			if err != nil {
				t.Fatalf("clientOpts: %v", err)
			}
			cl, err := kgo.NewClient(opts...)
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}
			cl.Close()
		})
	}

	// The plaintext default adds no options beyond the original three.
	opts, err := clientOpts(base)
	if err != nil {
		t.Fatalf("clientOpts: %v", err)
	}
	if len(opts) != 3 {
		t.Fatalf("plaintext default builds %d options, want 3 (brokers, topic, auto-create)", len(opts))
	}
}

// TestEnumSetsMatchConfig pins config's validation sets to what the producer
// actually supports, so the two enumerations cannot drift: every value config
// accepts must build options, and every codec the producer knows must be
// accepted by config.
func TestEnumSetsMatchConfig(t *testing.T) {
	t.Setenv("KAFKA_TEST_USER", "svc")
	t.Setenv("KAFKA_TEST_PASS", "s3cret")

	for comp := range config.KafkaCompressions {
		cfg := config.Kafka{Brokers: []string{"b:9092"}, Topic: "t", Compression: comp}
		if _, err := clientOpts(cfg); err != nil {
			t.Errorf("config accepts compression %q but clientOpts rejects it: %v", comp, err)
		}
	}
	for k := range codecs {
		if !config.KafkaCompressions[k] {
			t.Errorf("producer supports codec %q but config validation rejects it", k)
		}
	}
	for mech := range config.KafkaSASLMechanisms {
		cfg := config.Kafka{Brokers: []string{"b:9092"}, Topic: "t", TLS: true,
			SASLMechanism: mech, UsernameEnv: "KAFKA_TEST_USER", PasswordEnv: "KAFKA_TEST_PASS"}
		if _, err := clientOpts(cfg); err != nil {
			t.Errorf("config accepts sasl_mechanism %q but clientOpts rejects it: %v", mech, err)
		}
	}
}
