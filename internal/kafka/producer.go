// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"fmt"
	"log"

	"github.com/tbotnz/gnmi-kafka-producer/internal/config"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer struct {
	client *kgo.Client
	topic  string
}

func NewProducer(cfg config.Kafka) (*Producer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &Producer{client: cl, topic: cfg.Topic}, nil
}

func (p *Producer) Send(ctx context.Context, key, value []byte) {
	p.client.Produce(ctx, &kgo.Record{Topic: p.topic, Key: key, Value: value}, func(_ *kgo.Record, err error) {
		if err != nil {
			log.Printf("kafka produce err: %v", err)
		}
	})
}

func (p *Producer) Close(ctx context.Context) {
	if err := p.client.Flush(ctx); err != nil {
		log.Printf("kafka flush: %v", err)
	}
	p.client.Close()
}
