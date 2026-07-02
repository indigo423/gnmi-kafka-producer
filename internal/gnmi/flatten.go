// SPDX-License-Identifier: Apache-2.0

package gnmi

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
)

// Record is one enriched sample produced to Kafka. Key is the Kafka message key
// (device|interface, for stable per-interface ordering); Fields is the JSON
// body. The body carries the last-known value of every leaf seen for the
// interface (e.g. "in_octets", "in_octets_bps", "oper_status"), not just the
// leaf that triggered it: the Grafana Kafka datasource streams each message as
// a data frame and drops fields absent from the latest schema, so the field
// set must stay identical across messages.
type Record struct {
	Key    string
	Fields map[string]any
}

// MarshalJSON emits just the enriched body (the Key is the Kafka message key).
func (r Record) MarshalJSON() ([]byte, error) { return json.Marshal(r.Fields) }

// Enricher turns gNMI Notifications into enriched Records, holding the per-series
// state needed to compute counter rates and the per-interface merged state
// emitted with every Record. It is NOT safe for concurrent use; the gateway
// gives each host its own Enricher (one per runHost goroutine).
type Enricher struct {
	last  map[string]counterSample
	state map[string]map[string]any // device|interface → last-known metric values
}

type counterSample struct {
	value float64
	ts    time.Time
}

func NewEnricher() *Enricher {
	return &Enricher{
		last:  make(map[string]counterSample),
		state: make(map[string]map[string]any),
	}
}

// FromNotification flattens a single gNMI Notification into enriched Records —
// one per Update. Deletes are treated as control events: they evict rate state
// for their path and produce no metric record.
func (e *Enricher) FromNotification(device string, n *gnmipb.Notification) []Record {
	if n == nil {
		return nil
	}
	ts := time.Unix(0, n.GetTimestamp()).UTC()
	tsStr := ts.Format(time.RFC3339Nano)
	prefix := n.GetPrefix()

	out := make([]Record, 0, len(n.GetUpdate()))
	for _, u := range n.GetUpdate() {
		iface, leaf, _ := parsePath(prefix, u.GetPath())
		if rec := e.enrich(device, iface, leaf, u.GetVal(), ts, tsStr); rec != nil {
			out = append(out, *rec)
		}
	}
	for _, d := range n.GetDelete() {
		iface, leaf, _ := parsePath(prefix, d)
		delete(e.last, seriesKey(device, iface, leaf)) // bound the state map
		metric := strings.ReplaceAll(leaf, "-", "_")
		if st, ok := e.state[stateKey(device, iface)]; ok {
			delete(st, metric)
			delete(st, metric+"_bps")
			if len(st) == 0 {
				delete(e.state, stateKey(device, iface))
			}
		}
	}
	return out
}

// enrich merges a single leaf Update into the interface's state and builds one
// Record carrying the full last-known state, so every Record has the same
// field set regardless of which leaf triggered it.
func (e *Enricher) enrich(device, iface, leaf string, tv *gnmipb.TypedValue, ts time.Time, tsStr string) *Record {
	metric := strings.ReplaceAll(leaf, "-", "_")
	raw := EncodeValue(tv)

	sk := stateKey(device, iface)
	st := e.state[sk]
	if st == nil {
		st = make(map[string]any)
		e.state[sk] = st
	}

	switch leaf {
	case "oper-status", "admin-status":
		// Status is a string enum; map to numeric so it is chartable (UP=1).
		// JSON_IETF may qualify the enum with its YANG module
		// ("openconfig-interfaces:UP"), so compare only the local part.
		s, ok := stringVal(raw)
		if !ok {
			return nil
		}
		if i := strings.LastIndexByte(s, ':'); i >= 0 {
			s = s[i+1:]
		}
		st[metric] = boolToInt(strings.EqualFold(s, "UP"))
	default:
		num, f, ok := numeric(raw)
		if !ok {
			// Non-numeric, non-status leaf: pass the value through unchanged.
			st[metric] = raw
			break
		}
		st[metric] = num
		// Octet counters are monotonic — emit a bits/sec rate for throughput.
		if strings.Contains(leaf, "octets") {
			if bps, ok := e.rate(seriesKey(device, iface, leaf), f, ts); ok {
				st[metric+"_bps"] = bps
			} else {
				// First sample or counter reset: the last rate no longer holds.
				delete(st, metric+"_bps")
			}
		}
	}

	fields := make(map[string]any, len(st)+3)
	for k, v := range st {
		fields[k] = v
	}
	fields["device"] = device
	fields["timestamp"] = tsStr
	if iface != "" {
		fields["interface"] = iface
	}
	return &Record{Key: sk, Fields: fields}
}

// rate returns the per-second bit rate between the previous and current counter
// sample, updating the stored sample. It returns ok=false on the first sample
// (nothing to difference) and on a counter reset (new < previous), in which case
// the baseline is reset to the new value and no rate is emitted for this tick.
func (e *Enricher) rate(key string, value float64, ts time.Time) (float64, bool) {
	prev, seen := e.last[key]
	e.last[key] = counterSample{value: value, ts: ts}
	if !seen || value < prev.value {
		return 0, false
	}
	dt := ts.Sub(prev.ts).Seconds()
	if dt <= 0 {
		return 0, false
	}
	return (value - prev.value) / dt * 8, true
}

func seriesKey(device, iface, leaf string) string {
	return device + "|" + iface + "|" + leaf
}

func stateKey(device, iface string) string {
	return device + "|" + iface
}

// parsePath walks prefix then path elems, returning the interface name (from an
// interface[name=...] elem, empty if absent), the leaf (last elem name), and the
// rendered full path.
func parsePath(prefix, p *gnmipb.Path) (iface, leaf, full string) {
	var b strings.Builder
	render := func(path *gnmipb.Path) {
		if path == nil {
			return
		}
		for _, e := range path.GetElem() {
			b.WriteByte('/')
			b.WriteString(e.GetName())
			for k, v := range e.GetKey() {
				fmt.Fprintf(&b, "[%s=%s]", k, v)
				if e.GetName() == "interface" && k == "name" {
					iface = v
				}
			}
			leaf = e.GetName()
		}
	}
	render(prefix)
	render(p)
	if b.Len() == 0 {
		return iface, leaf, "/"
	}
	return iface, leaf, b.String()
}

// numeric parses a value that gNMI may encode either as a JSON number (12) or,
// for large uint64 counters under JSON_IETF, as a JSON string ("12"). It returns
// the value as a json.Number (marshals as a bare number, no precision loss) for
// emission, and as a float64 for rate math.
func numeric(raw json.RawMessage) (json.Number, float64, bool) {
	s := strings.TrimSpace(string(raw))
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return "", 0, false
	}
	return json.Number(s), f, true
}

func stringVal(raw json.RawMessage) (string, bool) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return s, true
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func EncodeValue(tv *gnmipb.TypedValue) json.RawMessage {
	if tv == nil {
		return json.RawMessage("null")
	}
	switch v := tv.GetValue().(type) {
	case *gnmipb.TypedValue_StringVal:
		return mustJSON(v.StringVal)
	case *gnmipb.TypedValue_IntVal:
		return mustJSON(v.IntVal)
	case *gnmipb.TypedValue_UintVal:
		return mustJSON(v.UintVal)
	case *gnmipb.TypedValue_BoolVal:
		return mustJSON(v.BoolVal)
	case *gnmipb.TypedValue_FloatVal:
		return mustJSON(v.FloatVal) //nolint:staticcheck // deprecated upstream but still emitted by some targets; handled for compatibility
	case *gnmipb.TypedValue_DoubleVal:
		return mustJSON(v.DoubleVal)
	case *gnmipb.TypedValue_BytesVal:
		return mustJSON(base64.StdEncoding.EncodeToString(v.BytesVal))
	case *gnmipb.TypedValue_AsciiVal:
		return mustJSON(v.AsciiVal)
	case *gnmipb.TypedValue_JsonIetfVal:
		if json.Valid(v.JsonIetfVal) {
			return json.RawMessage(v.JsonIetfVal)
		}
		return mustJSON(string(v.JsonIetfVal))
	case *gnmipb.TypedValue_JsonVal:
		if json.Valid(v.JsonVal) {
			return json.RawMessage(v.JsonVal)
		}
		return mustJSON(string(v.JsonVal))
	case *gnmipb.TypedValue_LeaflistVal:
		items := make([]json.RawMessage, 0, len(v.LeaflistVal.GetElement()))
		for _, e := range v.LeaflistVal.GetElement() {
			items = append(items, EncodeValue(e))
		}
		return mustJSON(items)
	default:
		log.Printf("unhandled TypedValue variant: %T", v)
		return json.RawMessage("null")
	}
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage("null")
	}
	return b
}
