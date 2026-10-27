# RelayEdge

**RelayEdge** is a lightweight, backpressure-aware HTTP/1.1 reverse proxy and load balancer written in TypeScript using Node.js core networking and streaming primitives (`node:http`, `node:net`, `node:stream`, `node:perf_hooks`) with **zero runtime dependencies**.

It is designed to demonstrate how Node.js operates beneath application frameworks: non-blocking socket I/O, stream backpressure propagation across TCP hops, HTTP/1.1 connection pooling, fault isolation, and Event Loop observability.

---

## Engineering Highlights

- **Zero-Buffering Stream Forwarding**: Streams request and response bodies directly between client and upstream sockets using `Readable.pipe()` and `stream.pipeline()`, maintaining bounded memory usage (`O(highWaterMark)`) even when fast upstreams serve multi-megabyte payloads to slow clients.
- **RFC 9110 / RFC 9112 Header Semantics**: Strips static hop-by-hop headers (`Connection`, `Keep-Alive`, `Transfer-Encoding`, `TE`, `Trailer`, `Upgrade`, `Proxy-*`) as well as headers dynamically nominated inside the `Connection` token list, while protecting proxy-injected headers (`Host`, `X-Forwarded-For`, `X-Forwarded-Host`, `X-Forwarded-Proto`, `X-Request-Id`, `Via`) against `Connection` header-stripping attacks.
- **Per-Upstream Keep-Alive Pooling (`LIFO`)**: Maintains dedicated `http.Agent` instances per upstream with `keepAlive: true`, `TCP_NODELAY` (`socket.setNoDelay(true)`), and `lifo` socket scheduling to maximize warm socket reuse and prevent ephemeral port exhaustion (`TIME_WAIT` accumulation).
- **Load Balancing**: Supports deterministic `round-robin` ($O(1)$ unsigned modular rotation) and `least-connections` (with rotating tie-breaking so idle pools distribute evenly rather than biasing toward index `0`).
- **Active & Passive Health Checks with Hysteresis**: Combines periodic out-of-band HTTP health probes (`GET /healthz`) with passive live-traffic failure tracking, guarded by configurable `unhealthyThreshold` and `healthyThreshold` counters to prevent upstream flapping.
- **Safe Idempotent Retries**: Automatically retries failed upstream connection attempts on alternative healthy upstreams strictly for idempotent, bodyless HTTP methods (`GET`, `HEAD`, `OPTIONS`) when failure occurs before response headers are received—never blindly retrying `POST`/`PUT`/`PATCH`/`DELETE` requests.
- **Fail-Fast Load Shedding**: Enforces global (`maxActiveRequests`) and per-upstream (`maxActiveRequestsPerUpstream`) concurrency limits before dispatching to `http.Agent`, shedding excess load immediately with `503 Service Unavailable` instead of accumulating unbounded in-memory queues in `agent.requests`.
- **Graceful Shutdown & Connection Draining**: Stops accepting new connections, closes idle Keep-Alive sockets immediately via `server.closeIdleConnections()`, injects `Connection: close` on in-flight responses so active connections close upon completion, and enforces a hard `shutdownGracePeriodMs` deadline.
- **Runtime & Event Loop Observability**: Tracks Event Loop delay percentiles (`monitorEventLoopDelay`), Event Loop Utilization (`performance.eventLoopUtilization()`), process memory, backpressure `drain` counts, and bounded-memory latency percentiles (`Float64Array` circular ring buffer) via `/__relay_edge/metrics` and `/__relay_edge/healthz`.

---

## Architecture & Module Layout

```text
src/
├── config/
│   └── validator.ts          # Strict configuration validation & loop prevention
├── http/
│   ├── headers.ts            # Hop-by-hop stripping, Connection token parser, X-Forwarded-*
│   └── errors.ts             # Error taxonomy (400/405/413/502/503/504) & safe responder
├── pool/
│   ├── upstream-pool.ts      # Per-upstream http.Agent lifecycle & health state machine
│   ├── load-balancer.ts      # Round-Robin & Least-Connections with capacity filtering
│   └── health-checker.ts     # Periodic active HTTP probing with hysteresis
├── proxy/
│   ├── forwarder.ts          # Streaming request/response forwarding, retries & timeouts
│   └── proxy-server.ts       # HTTP server lifecycle, load shedding & graceful drain
├── observability/
│   ├── ring-buffer.ts        # Fixed-size Float64Array circular buffer for p50/p95/p99
│   ├── metrics.ts            # Event Loop delay, ELU, memory & request telemetry
│   └── logger.ts             # Zero-dependency NDJSON structured logger
├── experiments/              # Reproducible Node.js runtime internals experiments
└── benchmarks/               # Direct vs Proxy throughput & latency benchmark suite
```

---

## Request Lifecycle & Backpressure Flow

1. **Ingress & Validation**: `ProxyServer` accepts the client request, checks global concurrency (`maxActiveRequests`), validates the HTTP method and origin-form path, and checks `Content-Length` against `maxRequestBodyBytes`.
2. **Upstream Selection**: `LoadBalancer` picks a healthy upstream from `UpstreamPool` that is below `maxActiveRequestsPerUpstream` using either `round-robin` or `least-connections`.
3. **Header Sanitization**: `buildUpstreamRequestHeaders` removes static and `Connection`-nominated hop-by-hop headers, rewrites `Host` to the upstream target, and sets `X-Forwarded-For`, `X-Forwarded-Host`, `X-Forwarded-Proto`, `X-Request-Id`, and `Via`.
4. **Outbound Request Streaming**: `forwardRequest` dispatches `http.request` using the upstream's Keep-Alive `http.Agent` and streams the client request body via `req.pipe(upstreamReq)`.
5. **Response Streaming & Backpressure**: Once upstream headers arrive, sanitized headers are written to the client (`res.writeHead`) and the body is streamed via `stream.pipeline(upstreamRes, res)`. If a slow client causes `res.write(chunk)` to exceed `highWaterMark`, `upstreamRes` pauses reading from the upstream socket (`uv_read_stop`), propagating TCP flow control back to the upstream server until the client socket emits `'drain'`.
6. **Fault Handling & Cancellation**: If the client disconnects prematurely, `upstreamReq.destroy()` is triggered immediately. If an upstream connection fails before sending response headers on a `GET`/`HEAD`/`OPTIONS` request, RelayEdge retries on another healthy upstream.

---

## Quick Start

### Prerequisites

- **Node.js** `>= 22.0.0`
- **npm** `>= 10.0.0`

### 1. Install & Build

```bash
npm install
npm run build
```

### 2. Start the Demo Cluster (3 Upstreams + RelayEdge Proxy)

```bash
npm run demo
```

In another terminal, send requests through RelayEdge on port `8080`:

```bash
# Observe Round-Robin distribution across upstream-1, upstream-2, upstream-3
curl -i http://127.0.0.1:8080/api/orders
curl -i http://127.0.0.1:8080/api/orders
curl -i http://127.0.0.1:8080/api/orders

# Inspect live proxy & Event Loop telemetry
curl -s http://127.0.0.1:8080/__relay_edge/metrics | jq .

# Check proxy health status
curl -i http://127.0.0.1:8080/__relay_edge/healthz
```

### 3. Run Tests, Experiments, and Benchmarks

```bash
# Run full unit + real-HTTP-server integration test suite
npm test

# Run Node.js Internals Experiments
npm run exp:event-loop
npm run exp:backpressure
npm run exp:keepalive

# Run Performance Benchmark Suite
npm run benchmark
```

---

## Configuration Reference

RelayEdge is configured programmatically via `new ProxyServer(config)`:

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `host` | `string` | `'127.0.0.1'` | Proxy listen address |
| `port` | `number` | `8080` | Proxy listen port (`0` for OS-assigned ephemeral port) |
| `strategy` | `'round-robin' \| 'least-connections'` | `'round-robin'` | Upstream load-balancing algorithm |
| `upstreams` | `UpstreamConfig[]` | *(required)* | List of `{ id, host, port, healthCheckPath? }` targets |
| `pool.keepAlive` | `boolean` | `true` | Reuse upstream TCP connections via `http.Agent` |
| `pool.maxSocketsPerUpstream` | `number` | `128` | Max concurrent active sockets per upstream `host:port` |
| `pool.maxFreeSocketsPerUpstream` | `number` | `32` | Max idle Keep-Alive sockets retained per upstream |
| `pool.scheduling` | `'lifo' \| 'fifo'` | `'lifo'` | Socket reuse policy (`lifo` keeps hot sockets active) |
| `timeouts.upstreamResponseTimeoutMs` | `number` | `15000` | Deadline (ms) for upstream to deliver response headers (`504`) |
| `timeouts.upstreamSocketTimeoutMs` | `number` | `30000` | Idle socket timeout on upstream connections |
| `timeouts.shutdownGracePeriodMs` | `number` | `10000` | Max time (ms) to wait for in-flight requests during `close()` |
| `stream.highWaterMarkBytes` | `number` | `65536` | Stream buffer threshold (bytes) before pausing producer |
| `healthCheck.enabled` | `boolean` | `false` | Enable periodic active HTTP health checks |
| `healthCheck.path` | `string` | `'/healthz'` | Default HTTP path probed on each upstream |
| `healthCheck.unhealthyThreshold` | `number` | `3` | Consecutive failures required to mark upstream unhealthy |
| `healthCheck.healthyThreshold` | `number` | `2` | Consecutive probe successes required to mark upstream healthy |
| `limits.maxActiveRequests` | `number` | `2048` | Max global in-flight requests before shedding load (`503`) |
| `limits.maxActiveRequestsPerUpstream` | `number` | `512` | Max active requests per upstream before skipping/shedding (`503`) |
| `limits.maxRequestBodyBytes` | `number` | `10485760` | Max request payload size (`10 MiB`) before rejecting (`413`) |
| `limits.maxRetries` | `number` | `1` | Max retries on alternative healthy upstreams for `GET`/`HEAD`/`OPTIONS` |
| `observability.metricsPath` | `string` | `'/__relay_edge/metrics'` | Built-in JSON telemetry endpoint |
| `observability.healthPath` | `string` | `'/__relay_edge/healthz'` | Built-in health endpoint |
| `observability.logLevel` | `LogLevel` | `'silent'` | Structured NDJSON log verbosity (`debug`, `info`, `warn`, `error`, `silent`) |

---

## Verified Performance & Runtime Experiments

All measurements below were executed on the following environment and can be reproduced locally via `npm run benchmark` and `npm run exp:*`.

- **Hardware**: Apple M1 Pro (10 CPU cores, 16 GiB RAM)
- **OS**: macOS (`darwin/arm64`)
- **Runtime**: Node.js `v22.23.2`
- **Methodology**: Each benchmark run executes an explicit warm-up pass at full target concurrency before resetting metrics and recording the measured pass, ensuring V8 JIT compilation and initial TCP handshakes do not skew steady-state numbers. All client, proxy, and upstream servers run on loopback (`127.0.0.1`) on the same machine, so CPU time is shared across processes.

### 1. Proxy Throughput & Latency Overhead (`npm run benchmark`)

| Benchmark Scenario | Requests | Concurrency | Payload | Req/sec | Throughput | `p50` | `p95` | `p99` | Event Loop `p99` |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `direct-upstream-1kib` *(No Proxy)* | 1,500 | 24 | 1 KiB | `12,280` | `11.99 MiB/s` | `1.81 ms` | `2.90 ms` | `4.31 ms` | — |
| `relay-edge-1-upstream-1kib` | 1,500 | 24 | 1 KiB | `4,719` | `4.61 MiB/s` | `4.80 ms` | `7.40 ms` | `10.40 ms` | `12.51 ms` |
| `relay-edge-3-upstreams-rr-1kib` *(Round-Robin)* | 1,500 | 24 | 1 KiB | `5,805` | `5.67 MiB/s` | `3.99 ms` | `5.72 ms` | `6.09 ms` | `11.80 ms` |
| `relay-edge-3-upstreams-lc-1kib` *(Least-Conn)* | 1,500 | 24 | 1 KiB | `4,984` | `4.87 MiB/s` | `3.63 ms` | `13.70 ms` | `23.03 ms` | `20.64 ms` |
| `relay-edge-streaming-256kib` | 300 | 12 | 256 KiB | `1,694` | `423.46 MiB/s` | `6.57 ms` | `9.90 ms` | `12.46 ms` | `12.88 ms` |

**Engineering Observations:**
- Adding a user-space HTTP/1.1 reverse proxy hop introduces an additional socket read/parse and write cycle per direction, adding `~1.8–2.2 ms` to median (`p50`) latency on loopback.
- For large streaming responses (`256 KiB`), RelayEdge sustains **`423.46 MiB/s`** (`~3.4 Gbps`) over loopback on a single Node.js thread with a `p99` latency of `12.46 ms`.

### 2. Experiment 1 — Event Loop Blocking vs `node:worker_threads` (`npm run exp:event-loop`)

Measures 80 concurrent proxied requests while injecting synchronous CPU work (`pbkdf2Sync`, 90,000 iterations) every 20 requests on the main thread versus offloading the same computation to a `Worker` thread:

| Scenario | Requests | `p50` Latency | `p95` Latency | `p99` Latency | Event Loop Delay `p99` | ELU |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| `baseline-io` *(Non-Blocking)* | 80 | `6.04 ms` | `11.15 ms` | `11.17 ms` | `11.49 ms` | `0.48` |
| `main-thread-cpu-block` | 80 | `6.09 ms` | **`30.33 ms`** | **`31.93 ms`** | **`29.75 ms`** | **`0.70`** |
| `worker-thread-offload` | 80 | `5.91 ms` | **`10.13 ms`** | **`10.47 ms`** | **`11.40 ms`** | **`0.33`** |

**Takeaway:** Running synchronous CPU work on the main thread prevents `libuv` from re-entering the I/O Poll phase, tripling `p95`/`p99` latency for unrelated network requests. Offloading CPU-bound work to `node:worker_threads` restores main-thread Event Loop responsiveness to baseline.

### 3. Experiment 2 — Streaming Backpressure vs In-Memory Buffering (`npm run exp:backpressure`)

Streams `6 MiB` responses simultaneously to 4 slow clients (`24 MiB` total concurrent payload):

| Proxy Implementation | Concurrent Clients | Payload / Client | Peak Memory Delta | Backpressure `drain` Events |
| :--- | :---: | :---: | :---: | :---: |
| `naive-buffered-proxy` (`Buffer.concat`) | 4 | `6 MiB` | **`+58.37 MiB`** | `0` |
| `relay-edge-streaming-proxy` (`highWaterMark: 16 KiB`) | 4 | `6 MiB` | **`+11.95 MiB`** | **`385`** |

**Takeaway:** Buffering responses in memory consumes nearly `5x` more peak memory than streaming with backpressure, because `Buffer.concat` retains both the chunk list and the contiguous copy while waiting for the slow client.

### 4. Experiment 3 — Keep-Alive Pool (`LIFO`) vs Ephemeral Sockets (`npm run exp:keepalive`)

Executes 240 proxied requests at concurrency `6`:

| Upstream Pool Mode | Requests | Upstream TCP Handshakes | Throughput (`req/s`) | `p50` Latency | `p99` Latency |
| :--- | :---: | :---: | :---: | :---: | :---: |
| `ephemeral-no-keepalive` | 240 | **`240`** | `1,681 req/s` | `2.93 ms` | `20.04 ms` |
| `pooled-keepalive-lifo` | 240 | **`5`** | **`3,494 req/s`** | **`1.42 ms`** | **`3.71 ms`** |

**Takeaway:** Reusing sockets via `http.Agent` (`keepAlive: true`, `scheduling: 'lifo'`) reduces upstream TCP handshakes by **97.9%**, doubles request throughput, and cuts `p99` tail latency by **5.4x**.

---

## Key Design Decisions & Trade-offs

1. **Asymmetric Stream Forwarding (`req.pipe(upstreamReq)` vs `pipeline(upstreamRes, res)`)**:
   - In Node.js's `lib/_http_incoming.js`, calling `req.destroy()` on an `IncomingMessage` whose request body has not finished streaming (`!req.complete`) immediately destroys the underlying client `net.Socket`.
   - If `stream.pipeline(req, upstreamReq)` were used for the request direction and the upstream refused the connection (`ECONNREFUSED`) while the client was uploading a `POST` body, `pipeline` would call `req.destroy()`, resetting the client's TCP connection (`ECONNRESET`) before the proxy's `502 Bad Gateway` response could flush.
   - Therefore, RelayEdge uses `req.pipe(upstreamReq)` with explicit `req.unpipe(upstreamReq); req.resume()` on upstream failure so the client reliably receives a clean `502`/`504` HTTP response. Conversely, once response headers have been sent to the client (`res.headersSent === true`), `pipeline(upstreamRes, res)` is used so any mid-stream upstream truncation immediately destroys the client socket rather than delivering a silently truncated body.
2. **Fail-Fast Concurrency Limits vs `http.Agent` Request Queuing**:
   - When `http.Agent` reaches `maxSockets`, Node.js queues additional requests inside an unbounded array (`agent.requests[host:port]`), which can cause severe queueing delay and memory exhaustion during upstream brownouts. RelayEdge tracks `activeConnections` per upstream and sheds excess load immediately with `503 Service Unavailable`.
3. **Client Disconnect Detection on Bodyless Requests**:
   - For `GET` requests without a body, `req` (`IncomingMessage`) emits `'end'` as soon as headers are parsed. To detect a client disconnecting while waiting for a slow upstream response, RelayEdge listens for `res.on('close')` and checks `if (!res.writableEnded)` to abort `upstreamReq.destroy()` immediately.

---

## Testing

The test suite uses Node.js's built-in test runner (`node:test` and `node:assert/strict`) and spins up real local HTTP servers and raw TCP sockets on ephemeral ports (`127.0.0.1:0`):

- **`tests/unit/config.test.ts`**: Configuration defaults, duplicate ID/endpoint rejection, and self-proxying loop prevention.
- **`tests/unit/headers.test.ts`**: RFC 9110 hop-by-hop stripping, dynamic `Connection` header token parsing, and defense against `Connection: x-forwarded-for` header-stripping attacks.
- **`tests/unit/load-balancer.test.ts`**: `round-robin` and `least-connections` tie-breaking and unhealthy target filtering.
- **`tests/integration/proxy-phase1.test.ts`**: End-to-end HTTP method forwarding, `4 MiB` SHA-256 verified backpressure streaming to slow consumers, client abort propagation, `502`/`504` error handling, and `TRACE` / open-proxy rejection.
- **`tests/integration/proxy-phase2.test.ts`**: Active health-check hysteresis transitions, idempotent `GET` retry vs non-idempotent `POST` protection, `503` load shedding, `413` body size enforcement, and graceful shutdown connection draining.
- **`tests/integration/proxy-phase3.test.ts`**: `/__relay_edge/metrics` and `/__relay_edge/healthz` verification, structured NDJSON logging, `LatencyRingBuffer` percentiles, and automated verification of all 3 runtime experiments.

---

## Known Limitations & Future Improvements

RelayEdge is a focused systems engineering project and is **not** a drop-in replacement for NGINX, Envoy, or HAProxy. Current limitations include:

- **HTTP/1.1 Only**: Does not terminate TLS (`node:tls`), negotiate HTTP/2 (`node:http2`), or proxy HTTP `Upgrade` / WebSocket tunnels.
- **Single-Process Runtime**: Runs on a single Node.js Event Loop without multi-process `node:cluster` socket sharing (`SO_REUSEPORT`).
- **Static Upstream Registry**: Upstreams are configured at startup rather than discovered dynamically via DNS SRV records or a service registry.

### Potential Future Extensions

- TLS termination and upstream mTLS via `node:tls`.
- WebSocket / HTTP `101 Switching Protocols` bidirectional socket splicing (`req.on('upgrade')`).
- Multi-worker scaling via `node:cluster` with aggregated shared-memory (`SharedArrayBuffer`) metrics.
