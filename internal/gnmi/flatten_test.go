// SPDX-License-Identifier: Apache-2.0

package gnmi

import (
	"encoding/json"
	"testing"
	"time"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
)

// ifPath builds /interfaces/interface[name=<iface>]/state/<tail...>.
func ifPath(iface string, tail ...string) *gnmipb.Path {
	elems := []*gnmipb.PathElem{
		{Name: "interfaces"},
		{Name: "interface", Key: map[string]string{"name": iface}},
		{Name: "state"},
	}
	for _, t := range tail {
		elems = append(elems, &gnmipb.PathElem{Name: t})
	}
	return &gnmipb.Path{Elem: elems}
}

func jsonIetf(s string) *gnmipb.TypedValue {
	return &gnmipb.TypedValue{Value: &gnmipb.TypedValue_JsonIetfVal{JsonIetfVal: []byte(s)}}
}

// counterNotif builds a Notification with one eth0 in-octets update at ts. The
// value is JSON_IETF string-encoded ("1000"), as nl6 emits large uint64s.
func counterNotif(octets string, ts time.Time) *gnmipb.Notification {
	return &gnmipb.Notification{
		Timestamp: ts.UnixNano(),
		Update: []*gnmipb.Update{
			{Path: ifPath("eth0", "counters", "in-octets"), Val: jsonIetf(`"` + octets + `"`)},
		},
	}
}

func oneRecord(t *testing.T, recs []Record) Record {
	t.Helper()
	if len(recs) != 1 {
		t.Fatalf("want 1 record, got %d", len(recs))
	}
	return recs[0]
}

func TestParsePath(t *testing.T) {
	tests := []struct {
		name      string
		path      *gnmipb.Path
		wantIface string
		wantLeaf  string
		wantFull  string
	}{
		{
			name:      "interface counter leaf",
			path:      ifPath("eth0", "counters", "in-octets"),
			wantIface: "eth0",
			wantLeaf:  "in-octets",
			wantFull:  "/interfaces/interface[name=eth0]/state/counters/in-octets",
		},
		{
			name:      "no key elems",
			path:      &gnmipb.Path{Elem: []*gnmipb.PathElem{{Name: "system"}, {Name: "hostname"}}},
			wantIface: "",
			wantLeaf:  "hostname",
			wantFull:  "/system/hostname",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			iface, leaf, full := parsePath(nil, tc.path)
			if iface != tc.wantIface || leaf != tc.wantLeaf || full != tc.wantFull {
				t.Fatalf("parsePath = (%q,%q,%q), want (%q,%q,%q)",
					iface, leaf, full, tc.wantIface, tc.wantLeaf, tc.wantFull)
			}
		})
	}
}

func TestRate(t *testing.T) {
	e := NewEnricher()
	base := time.Unix(1_700_000_000, 0).UTC()

	// First sample: raw counter present, no rate yet.
	r1 := oneRecord(t, e.FromNotification("dev1", counterNotif("1000", base)))
	if r1.Key != "dev1|eth0" {
		t.Fatalf("key = %q", r1.Key)
	}
	if got, ok := r1.Fields["in_octets"].(json.Number); !ok || got != "1000" {
		t.Fatalf("in_octets = %v (%T), want json.Number 1000", r1.Fields["in_octets"], r1.Fields["in_octets"])
	}
	if _, ok := r1.Fields["in_octets_bps"]; ok {
		t.Fatalf("first sample should have no _bps, got %v", r1.Fields["in_octets_bps"])
	}
	if r1.Fields["interface"] != "eth0" || r1.Fields["device"] != "dev1" {
		t.Fatalf("labels = %v", r1.Fields)
	}

	// Second sample 5s later: rate = (6000-1000)/5 * 8 = 8000 bps.
	r2 := oneRecord(t, e.FromNotification("dev1", counterNotif("6000", base.Add(5*time.Second))))
	if got, ok := r2.Fields["in_octets_bps"].(float64); !ok || got != 8000 {
		t.Fatalf("in_octets_bps = %v, want 8000", r2.Fields["in_octets_bps"])
	}

	// Counter reset (new < prev): no rate emitted, baseline resets.
	r3 := oneRecord(t, e.FromNotification("dev1", counterNotif("3000", base.Add(10*time.Second))))
	if _, ok := r3.Fields["in_octets_bps"]; ok {
		t.Fatalf("reset should emit no _bps, got %v", r3.Fields["in_octets_bps"])
	}

	// Next sample rates off the reset baseline: (5000-3000)/5 * 8 = 3200 bps.
	r4 := oneRecord(t, e.FromNotification("dev1", counterNotif("5000", base.Add(15*time.Second))))
	if got, ok := r4.Fields["in_octets_bps"].(float64); !ok || got != 3200 {
		t.Fatalf("in_octets_bps after reset = %v, want 3200", r4.Fields["in_octets_bps"])
	}
}

func TestStatusMapping(t *testing.T) {
	e := NewEnricher()
	ts := time.Unix(1_700_000_000, 0).UTC()
	// JSON_IETF enums may carry a YANG module prefix, as nl6 emits them.
	cases := map[string]int{"UP": 1, "openconfig-interfaces:UP": 1, "DOWN": 0, "openconfig-interfaces:DOWN": 0, "TESTING": 0}
	for val, want := range cases {
		n := &gnmipb.Notification{
			Timestamp: ts.UnixNano(),
			Update: []*gnmipb.Update{
				{Path: ifPath("eth1", "oper-status"), Val: jsonIetf(`"` + val + `"`)},
			},
		}
		rec := oneRecord(t, e.FromNotification("dev1", n))
		if rec.Fields["interface"] != "eth1" {
			t.Fatalf("interface label = %v, want eth1", rec.Fields["interface"])
		}
		if got, ok := rec.Fields["oper_status"].(int); !ok || got != want {
			t.Fatalf("oper_status(%q) = %v, want %d", val, rec.Fields["oper_status"], want)
		}
	}
}

// TestMergedState verifies that every Record carries the last-known value of
// every leaf seen for the interface, keeping the field set identical across
// messages (the Grafana Kafka datasource drops fields absent from a message).
func TestMergedState(t *testing.T) {
	e := NewEnricher()
	base := time.Unix(1_700_000_000, 0).UTC()

	e.FromNotification("dev1", counterNotif("1000", base))

	status := &gnmipb.Notification{
		Timestamp: base.Add(time.Second).UnixNano(),
		Update: []*gnmipb.Update{
			{Path: ifPath("eth0", "oper-status"), Val: jsonIetf(`"UP"`)},
		},
	}
	rec := oneRecord(t, e.FromNotification("dev1", status))

	// The status update's record must still carry the earlier counter value.
	if got, ok := rec.Fields["in_octets"].(json.Number); !ok || got != "1000" {
		t.Fatalf("in_octets = %v (%T), want json.Number 1000 carried over", rec.Fields["in_octets"], rec.Fields["in_octets"])
	}
	if got, ok := rec.Fields["oper_status"].(int); !ok || got != 1 {
		t.Fatalf("oper_status = %v, want 1", rec.Fields["oper_status"])
	}

	// A later counter update must carry the status back.
	r2 := oneRecord(t, e.FromNotification("dev1", counterNotif("6000", base.Add(5*time.Second))))
	if got, ok := r2.Fields["oper_status"].(int); !ok || got != 1 {
		t.Fatalf("oper_status = %v, want 1 carried over", r2.Fields["oper_status"])
	}
	if got, ok := r2.Fields["in_octets_bps"].(float64); !ok || got != 8000 {
		t.Fatalf("in_octets_bps = %v, want 8000", r2.Fields["in_octets_bps"])
	}

	// State is per interface: another interface must not see eth0's values.
	other := &gnmipb.Notification{
		Timestamp: base.Add(6 * time.Second).UnixNano(),
		Update: []*gnmipb.Update{
			{Path: ifPath("eth1", "oper-status"), Val: jsonIetf(`"DOWN"`)},
		},
	}
	r3 := oneRecord(t, e.FromNotification("dev1", other))
	if _, ok := r3.Fields["in_octets"]; ok {
		t.Fatalf("eth1 record must not carry eth0 state, got %v", r3.Fields)
	}
}

func TestStateEvictionOnDelete(t *testing.T) {
	e := NewEnricher()
	base := time.Unix(1_700_000_000, 0).UTC()
	key := "dev1|eth0|in-octets"

	e.FromNotification("dev1", counterNotif("1000", base))
	if _, ok := e.last[key]; !ok {
		t.Fatalf("expected rate state for %q after first sample", key)
	}

	del := &gnmipb.Notification{
		Timestamp: base.Add(1 * time.Second).UnixNano(),
		Delete:    []*gnmipb.Path{ifPath("eth0", "counters", "in-octets")},
	}
	if recs := e.FromNotification("dev1", del); len(recs) != 0 {
		t.Fatalf("delete should emit no records, got %d", len(recs))
	}
	if _, ok := e.last[key]; ok {
		t.Fatalf("expected state for %q to be evicted after delete", key)
	}
	if st, ok := e.state["dev1|eth0"]; ok {
		t.Fatalf("expected merged state to be evicted after delete, got %v", st)
	}

	// After eviction the next sample is treated as the first → no rate.
	r := oneRecord(t, e.FromNotification("dev1", counterNotif("9000", base.Add(6*time.Second))))
	if _, ok := r.Fields["in_octets_bps"]; ok {
		t.Fatalf("post-eviction sample should have no _bps, got %v", r.Fields["in_octets_bps"])
	}
}
