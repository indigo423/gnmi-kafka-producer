// SPDX-License-Identifier: Apache-2.0

// Package metrics holds the gateway's Prometheus collectors and the /metrics
// HTTP listener. Callers go through the helper functions so the metric names
// and label sets live in one place.
package metrics

import (
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	subscriptionUp = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gateway_subscription_up",
		Help: "1 once the profile's subscription has delivered a response and no subscribe error has been seen since; 0 otherwise. Pair with rate(gateway_records_produced_total) to detect silent stalls.",
	}, []string{"target", "profile"})

	subscribeErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_subscribe_errors_total",
		Help: "gNMI subscribe errors, per target and subscription profile.",
	}, []string{"target", "profile"})

	recordsProduced = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_records_produced_total",
		Help: "Enriched records successfully produced to Kafka (broker-acknowledged), per target.",
	}, []string{"target"})

	kafkaProduceErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_kafka_produce_errors_total",
		Help: "Kafka produce attempts that completed with an error.",
	})

	dialFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_dial_failures_total",
		Help: "gNMI dial attempts that failed (each is followed by a retry), per target.",
	}, []string{"target"})

	dialoutStreamsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gateway_dialout_streams_active",
		Help: "Currently open gNMIReverse Publish streams on the dial-out listener.",
	})

	dialoutUpdatesReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_dialout_updates_received_total",
		Help: "Dial-out notifications accepted, per resolved registry device name.",
	}, []string{"target"})

	// Deliberately unlabelled: the incoming target string is peer-controlled
	// and would be an unbounded label cardinality leak. The offending value is
	// logged instead.
	dialoutUnknownTarget = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_dialout_unknown_target_total",
		Help: "Dial-out notifications dropped because Prefix.Target matched no dialout.devices entry (value appears in the gateway log).",
	})
)

// SetSubscriptionUp marks a target's subscription profile as streaming (true)
// or down (false).
func SetSubscriptionUp(target, profile string, up bool) {
	v := 0.0
	if up {
		v = 1
	}
	subscriptionUp.WithLabelValues(target, profile).Set(v)
}

func IncSubscribeError(target, profile string) {
	subscribeErrors.WithLabelValues(target, profile).Inc()
}

func IncRecordsProduced(target string) {
	recordsProduced.WithLabelValues(target).Inc()
}

func IncKafkaProduceError() {
	kafkaProduceErrors.Inc()
}

func IncDialFailure(target string) {
	dialFailures.WithLabelValues(target).Inc()
}

// DialoutStreamOpened/Closed track the active Publish stream gauge.
func DialoutStreamOpened() { dialoutStreamsActive.Inc() }
func DialoutStreamClosed() { dialoutStreamsActive.Dec() }

func IncDialoutUpdateReceived(target string) {
	dialoutUpdatesReceived.WithLabelValues(target).Inc()
}

func IncDialoutUnknownTarget() {
	dialoutUnknownTarget.Inc()
}

// Serve blocks serving /metrics on the given port; run it in a goroutine.
func Serve(port int) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return srv.ListenAndServe()
}
