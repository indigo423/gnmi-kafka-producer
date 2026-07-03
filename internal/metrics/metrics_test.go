// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestSubscriptionHealthTransitions exercises the D2 semantics: profiles start
// down, go up on attributed updates, and drop back on attributed errors while
// the error counter climbs — per target+profile, independent of siblings.
func TestSubscriptionHealthTransitions(t *testing.T) {
	up := func(profile string) float64 {
		return testutil.ToFloat64(subscriptionUp.WithLabelValues("t1", profile))
	}

	SetSubscriptionUp("t1", "counters", false)
	SetSubscriptionUp("t1", "status", false)
	if up("counters") != 0 || up("status") != 0 {
		t.Fatalf("initial gauges = %v/%v, want 0/0", up("counters"), up("status"))
	}

	SetSubscriptionUp("t1", "counters", true)
	IncSubscribeError("t1", "status")
	SetSubscriptionUp("t1", "status", false)
	if up("counters") != 1 {
		t.Fatalf("healthy profile gauge = %v, want 1", up("counters"))
	}
	if up("status") != 0 {
		t.Fatalf("failing profile gauge = %v, want 0", up("status"))
	}
	if got := testutil.ToFloat64(subscribeErrors.WithLabelValues("t1", "status")); got != 1 {
		t.Fatalf("subscribe_errors{status} = %v, want 1", got)
	}

	// Recovery: an attributed update brings the profile back up.
	SetSubscriptionUp("t1", "status", true)
	if up("status") != 1 {
		t.Fatalf("recovered profile gauge = %v, want 1", up("status"))
	}
}

func TestCounters(t *testing.T) {
	before := testutil.ToFloat64(recordsProduced.WithLabelValues("t2"))
	IncRecordsProduced("t2")
	IncRecordsProduced("t2")
	if got := testutil.ToFloat64(recordsProduced.WithLabelValues("t2")); got != before+2 {
		t.Fatalf("records_produced{t2} = %v, want %v", got, before+2)
	}

	beforeErr := testutil.ToFloat64(kafkaProduceErrors)
	IncKafkaProduceError()
	if got := testutil.ToFloat64(kafkaProduceErrors); got != beforeErr+1 {
		t.Fatalf("kafka_produce_errors = %v, want %v", got, beforeErr+1)
	}

	beforeDial := testutil.ToFloat64(dialFailures.WithLabelValues("t2"))
	IncDialFailure("t2")
	if got := testutil.ToFloat64(dialFailures.WithLabelValues("t2")); got != beforeDial+1 {
		t.Fatalf("dial_failures{t2} = %v, want %v", got, beforeDial+1)
	}
}
