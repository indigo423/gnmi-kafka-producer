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

    gconf --> GW
    GW -- Subscribe --> NL6
    GW -- produce JSON --> K
    K --> UI
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
    end

    GW -- gNMI --> NL6
    GW -- produce --> K
    UI -- read --> K
```

## Quickstart

```sh
make up                       # docker compose up -d --build
make ps                       # watch services come healthy
open http://localhost:8080    # kafbat: cluster "demo", topic "gnmi.telemetry"
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
          skip_verify: true, encoding: json_ietf, sample_interval: 5s }
paths:  [/interfaces/interface[name=*]/state/oper-status,
         /interfaces/interface[name=*]/state/counters/in-octets, ...]
hosts:  [192.168.100.1]
```

nl6 exposes the OpenConfig `interfaces` model (read-only) over gNMI on port 9339,
with a self-signed cert (`skip_verify: true`) and no authentication.

- **Add devices**: bump `-auto-count` on the `nl6` service in `e2e/compose.yml`
  and add the extra `192.168.100.x` addresses to `hosts:`. The gateway dials all
  hosts concurrently.
- **Change paths or sample interval**: edit `configs/gateway.yaml`, then
  `docker compose -f e2e/compose.yml restart gateway`. No rebuild. See
  [nl6's gNMI reference](https://nl6.eu) for the full leaf list (ifindex,
  admin/oper-status, last-change, and the complete `counters/*` set).
- **Point at a real device**: give the `gateway` its own network instead of
  `network_mode: "service:nl6"`, put the device address in `hosts:`, and ensure
  the gateway container can route to it.

## Output format

One JSON record per leaf Update, keyed by gNMI path:

```json
{
  "target":    "192.168.100.1",
  "path":      "/interfaces/interface[name=TenGigE0/0/0/0]/state/counters/out-octets",
  "value":     "89115667333884",
  "timestamp": "2026-06-26T08:10:01.234567890Z"
}
```

`target` is the host string from the config. `value` is the typed value from
the gNMI `TypedValue` oneof: scalars become JSON primitives, `JSON_IETF`
sub-trees pass through as objects.

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
│   └── compose.yml           # end-to-end demo stack
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
