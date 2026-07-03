// SPDX-License-Identifier: Apache-2.0

package exporter

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

const noStale = time.Hour

const record = `{
  "device": "192.168.100.1",
  "target": "nl6-dev-01",
  "role": "leaf",
  "interface": "eth0",
  "oper_status": 1,
  "in_octets": 89115667333884,
  "in_octets_bps": 8123.4,
  "timestamp": "2026-06-26T08:10:01.234567890Z"
}`

func TestUpdateCollect(t *testing.T) {
	s := NewStore(noStale)
	s.Update([]byte("192.168.100.1|eth0"), []byte(record))

	want := `
# HELP gnmi_in_octets Last-known value of the in_octets record field.
# TYPE gnmi_in_octets gauge
gnmi_in_octets{device="192.168.100.1",interface="eth0",role="leaf",target="nl6-dev-01"} 8.9115667333884e+13
# HELP gnmi_in_octets_bps Last-known value of the in_octets_bps record field.
# TYPE gnmi_in_octets_bps gauge
gnmi_in_octets_bps{device="192.168.100.1",interface="eth0",role="leaf",target="nl6-dev-01"} 8123.4
# HELP gnmi_oper_status Last-known value of the oper_status record field.
# TYPE gnmi_oper_status gauge
gnmi_oper_status{device="192.168.100.1",interface="eth0",role="leaf",target="nl6-dev-01"} 1
# HELP exporter_records_consumed_total Records consumed from the telemetry topic.
# TYPE exporter_records_consumed_total counter
exporter_records_consumed_total 1
# HELP exporter_decode_errors_total Records dropped because their JSON body could not be decoded.
# TYPE exporter_decode_errors_total counter
exporter_decode_errors_total 0
`
	if err := testutil.CollectAndCompare(s, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}

func TestUpdateReplacesState(t *testing.T) {
	s := NewStore(noStale)
	s.Update([]byte("k"), []byte(`{"interface":"eth0","in_octets":1,"in_octets_bps":8}`))
	// Counter reset: the gateway drops _bps from the merged state; replacing
	// (not merging) must drop the stale gauge too.
	s.Update([]byte("k"), []byte(`{"interface":"eth0","in_octets":2}`))

	want := `
# HELP gnmi_in_octets Last-known value of the in_octets record field.
# TYPE gnmi_in_octets gauge
gnmi_in_octets{interface="eth0"} 2
`
	if err := testutil.CollectAndCompare(s, strings.NewReader(want),
		"gnmi_in_octets", "gnmi_in_octets_bps"); err != nil {
		t.Fatal(err)
	}
}

func TestReservedLabelSkipped(t *testing.T) {
	s := NewStore(noStale)
	// "--zone" sanitizes to "__zone", which Prometheus reserves; keeping it
	// would make MustNewConstMetric panic the scrape.
	s.Update([]byte("k"), []byte(`{"interface":"eth0","--zone":"a","in_octets":1}`))

	want := `
# HELP gnmi_in_octets Last-known value of the in_octets record field.
# TYPE gnmi_in_octets gauge
gnmi_in_octets{interface="eth0"} 1
`
	if err := testutil.CollectAndCompare(s, strings.NewReader(want), "gnmi_in_octets"); err != nil {
		t.Fatal(err)
	}
}

func TestStaleSeriesSkipped(t *testing.T) {
	s := NewStore(time.Nanosecond)
	s.Update([]byte("k"), []byte(`{"interface":"eth0","in_octets":1}`))
	time.Sleep(time.Millisecond)

	if got := testutil.CollectAndCount(s, "gnmi_in_octets"); got != 0 {
		t.Fatalf("stale series still exported: %d families", got)
	}
}

func TestDecodeError(t *testing.T) {
	s := NewStore(noStale)
	s.Update([]byte("k"), []byte(`not json`))

	if got := testutil.CollectAndCount(s, "exporter_decode_errors_total"); got != 1 {
		t.Fatalf("decode_errors families = %d, want 1", got)
	}
	if got := testutil.ToFloat64(s.errors); got != 1 {
		t.Fatalf("decode_errors = %v, want 1", got)
	}
}

func TestSanitize(t *testing.T) {
	for in, want := range map[string]string{
		"in_octets": "in_octets",
		"rack-id":   "rack_id",
		"1abc":      "_abc",
	} {
		if got := sanitize(in); got != want {
			t.Errorf("sanitize(%q) = %q, want %q", in, got, want)
		}
	}
}
