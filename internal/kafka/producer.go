// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/tbotnz/gnmi-kafka-producer/internal/config"
	"github.com/tbotnz/gnmi-kafka-producer/internal/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type Producer struct {
	client *kgo.Client
	topic  string
}

func NewProducer(cfg config.Kafka) (*Producer, error) {
	opts, err := clientOpts(cfg)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &Producer{client: cl, topic: cfg.Topic}, nil
}

// clientOpts maps the config onto franz-go options. With none of the optional
// fields set this returns exactly the pre-hardening option set (plaintext, no
// auth). SASL credential values are read from the environment here — config
// validation already verified the variables are present.
func clientOpts(cfg config.Kafka) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.AllowAutoTopicCreation(),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}
	if cfg.Compression != "" {
		codec, ok := codecs[strings.ToLower(cfg.Compression)]
		if !ok {
			return nil, fmt.Errorf("kafka.compression: unknown value %q", cfg.Compression)
		}
		opts = append(opts, kgo.ProducerBatchCompression(codec))
	}
	if cfg.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{
			InsecureSkipVerify: cfg.TLSSkipVerify, // #nosec G402 -- explicit operator opt-in via kafka.tls_skip_verify
			MinVersion:         tls.VersionTLS12,
		}))
	}
	if cfg.SASLMechanism != "" {
		user, pass := os.Getenv(cfg.UsernameEnv), os.Getenv(cfg.PasswordEnv)
		switch strings.ToUpper(cfg.SASLMechanism) {
		case "PLAIN":
			opts = append(opts, kgo.SASL(plain.Auth{User: user, Pass: pass}.AsMechanism()))
		case "SCRAM-SHA-256":
			opts = append(opts, kgo.SASL(scram.Auth{User: user, Pass: pass}.AsSha256Mechanism()))
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(scram.Auth{User: user, Pass: pass}.AsSha512Mechanism()))
		default:
			return nil, fmt.Errorf("kafka.sasl_mechanism: unknown value %q", cfg.SASLMechanism)
		}
	}
	return opts, nil
}

// codecs is the compression source of truth: config validation checks against
// these keys (config.KafkaCompressions mirrors them; a producer test pins the
// two together), and clientOpts maps them to franz-go codecs.
var codecs = map[string]kgo.CompressionCodec{
	"none":   kgo.NoCompression(),
	"gzip":   kgo.GzipCompression(),
	"snappy": kgo.SnappyCompression(),
	"lz4":    kgo.Lz4Compression(),
	"zstd":   kgo.ZstdCompression(),
}

// Send enqueues one record; the produce is async. The outcome is counted in
// the completion callback so records_produced reflects broker-acknowledged
// records, not enqueues.
func (p *Producer) Send(ctx context.Context, target string, key, value []byte) {
	p.client.Produce(ctx, &kgo.Record{Topic: p.topic, Key: key, Value: value}, func(_ *kgo.Record, err error) {
		if err != nil {
			metrics.IncKafkaProduceError()
			log.Printf("kafka produce err: %v", err)
			return
		}
		metrics.IncRecordsProduced(target)
	})
}

func (p *Producer) Close(ctx context.Context) {
	if err := p.client.Flush(ctx); err != nil {
		log.Printf("kafka flush: %v", err)
	}
	p.client.Close()
}
