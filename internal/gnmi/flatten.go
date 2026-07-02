// SPDX-License-Identifier: Apache-2.0

package gnmi

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
)

// Record is the JSON shape emitted to Kafka, one per leaf Update or Delete.
type Record struct {
	Target    string          `json:"target"`
	Path      string          `json:"path"`
	Value     json.RawMessage `json:"value"`
	Timestamp string          `json:"timestamp"`
	Delete    bool            `json:"delete,omitempty"`
}

// FromNotification flattens a single gNMI Notification into 1..N Records.
// One Record per Update, one per Delete (value=null, delete=true).
func FromNotification(targetName string, n *gnmipb.Notification) []Record {
	if n == nil {
		return nil
	}
	ts := time.Unix(0, n.GetTimestamp()).UTC().Format(time.RFC3339Nano)
	prefix := n.GetPrefix()
	out := make([]Record, 0, len(n.GetUpdate())+len(n.GetDelete()))
	for _, u := range n.GetUpdate() {
		out = append(out, Record{
			Target:    targetName,
			Path:      JoinPath(prefix, u.GetPath()),
			Value:     EncodeValue(u.GetVal()),
			Timestamp: ts,
		})
	}
	for _, d := range n.GetDelete() {
		out = append(out, Record{
			Target:    targetName,
			Path:      JoinPath(prefix, d),
			Value:     json.RawMessage("null"),
			Timestamp: ts,
			Delete:    true,
		})
	}
	return out
}

func JoinPath(prefix, p *gnmipb.Path) string {
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
			}
		}
	}
	render(prefix)
	render(p)
	if b.Len() == 0 {
		return "/"
	}
	return b.String()
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
