// SPDX-License-Identifier: Apache-2.0

// Package exporter turns the merged per-interface JSON records on the
// gnmi.telemetry topic into Prometheus metrics: every numeric field becomes a
// gauge named gnmi_<field>, every string field (device, interface, target and
// the free-form target labels) becomes a Prometheus label. The Store holds the
// last-known record per Kafka key, so a scrape always sees the full current
// state regardless of scrape/message phase.
package exporter

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Store is the Kafka-side write model and the Prometheus Collector in one. It
// is safe for concurrent use (one consumer goroutine writing, scrapes reading).
type Store struct {
	mu     sync.RWMutex
	series map[string]series // Kafka key (device|interface) → last state

	records prometheus.Counter
	errors  prometheus.Counter
}

type series struct {
	labels map[string]string
	values map[string]float64
}

func NewStore() *Store {
	return &Store{
		series: make(map[string]series),
		records: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "exporter_records_consumed_total",
			Help: "Records consumed from the telemetry topic.",
		}),
		errors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "exporter_decode_errors_total",
			Help: "Records dropped because their JSON body could not be decoded.",
		}),
	}
}

// Update ingests one record body, replacing the state held for its Kafka key.
// The gateway emits the full merged interface state with every record, so
// replacing (not merging) tracks leaf deletes for free.
func (s *Store) Update(key, body []byte) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var fields map[string]any
	if err := dec.Decode(&fields); err != nil {
		s.errors.Inc()
		return
	}

	sr := series{labels: make(map[string]string), values: make(map[string]float64)}
	for k, v := range fields {
		if k == "timestamp" { // scrape time is the sample time
			continue
		}
		switch v := v.(type) {
		case json.Number:
			f, err := v.Float64()
			if err != nil {
				continue
			}
			sr.values[sanitize(k)] = f
		case string:
			sr.labels[sanitize(k)] = v
		}
		// Anything else (bool, array, object) has no metric mapping; skip.
	}

	s.mu.Lock()
	s.series[string(key)] = sr
	s.mu.Unlock()
	s.records.Inc()
}

// Describe sends nothing: the metric set follows the telemetry, so the Store
// registers as an unchecked collector.
func (s *Store) Describe(chan<- *prometheus.Desc) {}

func (s *Store) Collect(ch chan<- prometheus.Metric) {
	s.records.Collect(ch)
	s.errors.Collect(ch)

	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, sr := range s.series {
		keys := make([]string, 0, len(sr.labels))
		for k := range sr.labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		vals := make([]string, len(keys))
		for i, k := range keys {
			vals[i] = sr.labels[k]
		}
		for name, v := range sr.values {
			desc := prometheus.NewDesc("gnmi_"+name,
				"Last-known value of the "+name+" record field.", keys, nil)
			ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, v, vals...)
		}
	}
}

// sanitize maps a record field name onto the Prometheus name charset
// [a-zA-Z0-9_]. Gateway metric fields are already snake_case; this guards the
// free-form target label keys (e.g. "rack-id").
func sanitize(name string) string {
	var b strings.Builder
	for i, r := range name {
		switch {
		case r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r == '_':
			b.WriteRune(r)
		case r >= '0' && r <= '9' && i > 0:
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}
