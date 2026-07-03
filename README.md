# gnmi-kafka-producer

Docker Compose stack that streams gNMI telemetry from a network simulator into
Kafka. The gateway reads a single YAML config and can be deployed and
reconfigured independently.

```mermaid
flowchart LR
    gconf[configs/gateway.yaml]
    GW[gateway]
    NL6[(nl6 simulator)]
    K[(kafka)]
    UI[kafka-ui :8080]
    GF[grafana :3000]

    gconf --> GW
    GW -- Subscribe --> NL6
    GW -- produce JSON --> K
    K --> UI
    K -- Kafka datasource --> GF
```

## Components

```mermaid
flowchart TB
    subgraph sim["network simulator"]
        NL6["nl6<br/>ghcr.io/labmonkeys-space/nl6:latest<br/>gNMI :9339 (TLS), self-animating"]
    end

    subgraph producer["producer (Go, distroless)"]
        GW["gateway<br/>cmd/gateway<br/>gNMI Subscribe, flatten, produce to Kafka"]
    end

    subgraph transport["transport"]
        K["kafka<br/>apache/kafka:3.9.1<br/>single-node KRaft<br/>:9092 in-net, :29092 host"]
    end

    subgraph ui["UI"]
        UI["kafka-ui<br/>ghcr.io/kafbat/kafka-ui:latest<br/>kafbat web UI<br/>:8080 host"]
        GF["grafana<br/>grafana/grafana:13.1.0<br/>+ kafka-datasource plugin<br/>live dashboard :3000 host"]
    end

    GW -- gNMI --> NL6
    GW -- produce --> K
    UI -- read --> K
    GF -- stream (Kafka datasource) --> K
```

## Quickstart

```sh
make up                       # docker compose up -d --build
make ps                       # watch services come healthy
open http://localhost:8080    # kafbat: cluster "demo", topic "gnmi.telemetry"
open http://localhost:3000    # grafana: "gNMI Telemetry (live)" dashboard (anonymous)
```

[nl6](https://nl6.eu) boots in seconds and emits self-animating telemetry
(cycling interface counters, sine-wave CPU/mem/temp), so there is no separate
stimulus generator — the data moves on its own. The gateway shares nl6's network
namespace (`network_mode: "service:nl6"` in the compose file) so it can dial
nl6's per-device gNMI endpoints.

## Configuration

The gateway is configured by a single file, [`configs/gateway.yaml`](./configs).

```yaml
kafka:  { brokers: ["kafka:9092"], topic: gnmi.telemetry }
gnmi:   { port: 9339, username: "", password: "",
          skip_verify: true, encoding: json_ietf }
subscription_profiles:
  interface-counters:
    mode: SAMPLE
    sample_interval: 5s
    paths: [/interfaces/interface[name=*]/state/counters/in-octets, ...]
  interface-status:
    mode: ON_CHANGE
    heartbeat_interval: 5m
    paths: [/interfaces/interface[name=*]/state/oper-status, ...]
hosts:  [192.168.100.1]
```

Paths are grouped into named **subscription profiles**, each with its own
collection mode: `SAMPLE` re-reads its paths every `sample_interval`; `ON_CHANGE`
fires only on state transitions (plus an optional `heartbeat_interval` resend so
quiet leaves are still confirmed alive). Every host subscribes to every profile
over a single gNMI Subscribe stream. At startup the gateway rejects
oversubscribed configs — the same path twice, or a parent container together
with one of its own leaves (e.g. `.../state` plus `.../state/counters/in-octets`)
— since those make the device stream the same data more than once.

nl6 exposes the OpenConfig `interfaces` model (read-only) over gNMI on port 9339,
with a self-signed cert (`skip_verify: true`) and no authentication.

- **Add devices**: bump `-auto-count` on the `nl6` service in `e2e/compose.yml`
  and add the extra `192.168.100.x` addresses to `hosts:`. The gateway dials all
  hosts concurrently.
- **Change paths, modes, or intervals**: edit the `subscription_profiles` in
  `configs/gateway.yaml`, then `docker compose -f e2e/compose.yml restart gateway`.
  No rebuild. See [nl6's gNMI reference](https://nl6.eu) for the full leaf list
  (ifindex, admin/oper-status, last-change, and the complete `counters/*` set).
- **Point at a real device**: give the `gateway` its own network instead of
  `network_mode: "service:nl6"`, put the device address in `hosts:`, and ensure
  the gateway container can route to it.

## Output format

One JSON record per leaf Update, keyed by `device|interface`. Each record carries
the **full last-known state of its interface** — every leaf seen so far, not just
the leaf that triggered it — so the field set is identical across messages:

```json
{
  "device":         "192.168.100.1",
  "interface":      "TenGigE0/0/0/0",
  "admin_status":   1,
  "oper_status":    1,
  "in_octets":      89115667333884,
  "in_octets_bps":  8123.4,
  "out_octets":     90470118138447,
  "out_octets_bps": 9801963523.7,
  "timestamp":      "2026-06-26T08:10:01.234567890Z"
}
```

The stable field set is what makes the live dashboard work: the Grafana Kafka
datasource streams each message as a data frame, and Grafana's streaming buffer
drops any field that is missing from the latest message's schema. Per-metric
records (the previous shape) made the schema flip on every message and wiped the
numeric columns. The plugin does **not** turn string fields into series labels;
the dashboard splits per interface with a `partitionByValues` transformation on
`device` + `interface` instead.

- **Metric key** — the leaf name with `-`→`_` (e.g. `in_octets`), carrying the raw
  value as a JSON number.
- **Rate** — for octet counters, `<metric>_bps` = Δvalue ÷ Δt × 8 is computed at the
  source (the gateway keeps the last sample per series). It is omitted on the first
  sample and on a counter reset (and dropped from the merged state until the next
  valid delta).
- **Status** — `oper-status`/`admin-status` are emitted as numeric `oper_status`/
  `admin_status` (`UP` → 1, otherwise 0) so they are chartable. A YANG module
  prefix on the enum (`openconfig-interfaces:UP`, as nl6 sends it) is ignored.
- **Deletes** evict the leaf's rate and merged state and produce no record.

> **Breaking change**: this replaces the earlier one-metric-per-message record
> (and before that, the flat `{target, path, value}` record). Any consumer of the
> old shapes must be updated. Only `kafka-ui` (schema-agnostic) and the Grafana
> dashboard read this topic in the demo.

## Commands

```sh
make logs                                  # tail all services
make tail-topic                            # console-consumer dump of first 50 records
docker compose -f e2e/compose.yml logs -f gateway
docker compose -f e2e/compose.yml logs -f nl6
make down                                  # tear down
```

## Project layout

```
.
├── configs/
│   └── gateway.yaml          # gateway config
├── e2e/
│   ├── compose.yml           # end-to-end demo stack
│   └── grafana/              # provisioned datasource + live dashboard
├── Makefile
├── README.md
├── go.mod / go.sum
├── cmd/
│   └── gateway/              # subscribe loop, one goroutine per host
│       ├── Dockerfile
│       └── main.go
└── internal/
    ├── config/
    │   ├── config.go         # shared field types + YAML loader
    │   └── gateway.go        # Gateway type, LoadGateway, validate
    ├── gnmi/
    │   ├── client.go         # dial-with-retry, SubscribeRequest builder
    │   └── flatten.go        # gNMI Notification to []Record, TypedValue cases
    └── kafka/producer.go     # franz-go wrapper
```

## Notes

- nl6 puts each simulated device on its own IP inside a Linux TUN/network
  namespace, not on the container's default interface. The `gateway` joins that
  namespace via `network_mode: "service:nl6"` to reach `192.168.100.x:9339`;
  Kafka stays reachable because nl6 is on the compose bridge network.
- nl6's gNMI is read-only (Capabilities/Get/Subscribe; no Set) and serves TLS
  with a self-signed cert, so the gateway uses `skip_verify: true` and no
  credentials.
- Kafka data lives in the container layer. `make down` wipes everything.
- `kafka:3.9.1` is pinned. `nl6` and `kafka-ui` track `latest`. Change in
  `e2e/compose.yml`.
