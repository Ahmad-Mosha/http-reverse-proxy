# HTTP Reverse Proxy

[![CI](https://github.com/Ahmad-Mosha/http-reverse-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/Ahmad-Mosha/http-reverse-proxy/actions/workflows/ci.yml)

An HTTP/1.1 reverse proxy and load balancer written in TypeScript, using only
Node.js core modules (`node:http`, `node:net`, `node:stream`). It has no
runtime dependencies.

I built it to understand what happens under frameworks like Express and
NestJS: how sockets and streams behave, what backpressure really means, why
connection pooling matters, and what blocks the event loop. Each of those
questions has an experiment in this repo with its results below.

It is a learning project, not a replacement for nginx or HAProxy.

## What it does

- **Streams bodies in both directions** without buffering them in memory. If a
  client reads slowly, the proxy stops reading from the upstream until the
  client catches up.
- **Load balancing** across upstreams with round-robin or least-connections.
- **Keep-alive connection pools**, one `http.Agent` per upstream, so warm
  sockets are reused instead of opening a new TCP connection per request.
- **Health checks.** Active checks call `/healthz` on a timer, and passive
  checks count failures on real traffic. An upstream must fail several times
  in a row to be marked down, and succeed several times to come back, so it
  does not flap.
- **Safe retries.** If an upstream fails before sending response headers, a
  `GET`, `HEAD` or `OPTIONS` request is retried on another upstream. `POST`,
  `PUT`, `PATCH` and `DELETE` are never retried.
- **Load shedding.** Global and per-upstream limits on active requests. Over
  the limit, the proxy answers `503` right away instead of queueing requests in
  memory.
- **Correct headers.** Removes hop-by-hop headers, including any header named
  in `Connection`, and sets `X-Forwarded-For`, `X-Forwarded-Host`,
  `X-Forwarded-Proto`, `X-Request-Id` and `Via`. A client cannot use
  `Connection` to strip the headers the proxy adds.
- **Clear errors:** `400`, `405`, `413`, `502`, `503` and `504`.
- **Graceful shutdown.** Stops accepting connections, closes idle keep-alive
  sockets, lets in-flight requests finish, and gives up after a deadline.
- **Metrics** at `/__proxy/metrics`: latency percentiles, event loop delay
  and utilization, memory, and backpressure events. Health at
  `/__proxy/healthz`. Logs are JSON lines.

## How a request flows

1. The server checks the global request limit, the method, the path and
   `Content-Length`.
2. The load balancer picks a healthy upstream that is under its own limit.
3. Headers are cleaned and the forwarding headers are added.
4. The request goes out on a pooled socket, and the request body is piped to
   it.
5. When the upstream answers, the response headers are written to the client
   and the body is streamed with `stream.pipeline`.
6. If the client disconnects, the upstream request is destroyed. If the
   upstream fails before headers on a safe method, the request is retried on
   another upstream.

## Code layout

```text
src/
  config/          config defaults and validation (also rejects proxying to itself)
  http/            header handling and error responses
  pool/            upstream pools, load balancer, health checker
  proxy/           the server and the request forwarder
  observability/   metrics, latency ring buffer, JSON logger
  experiments/     the three runtime experiments below
  benchmarks/      direct vs proxied benchmark
  demo/            three demo upstreams behind the proxy
tests/
  unit/            config, headers, load balancer
  integration/     real HTTP servers and sockets on random ports
```

## Run it

Needs Node.js 22 or later.

```bash
npm install
npm run build
npm run demo
```

The demo starts three upstreams and the proxy on port 8080. In another
terminal:

```bash
curl -i http://127.0.0.1:8080/api/orders     # run it a few times to see round-robin
curl -s http://127.0.0.1:8080/__proxy/metrics
```

Tests, experiments and the benchmark:

```bash
npm test
npm run exp:event-loop
npm run exp:backpressure
npm run exp:keepalive
npm run benchmark
```

CI runs formatting, type checks, lint, all tests and the build on every push
and pull request.

## Configuration

The proxy is created in code with `new ProxyServer(config)`. The main options:

| Option | Default | Meaning |
|---|---|---|
| `strategy` | `round-robin` | or `least-connections` |
| `upstreams` | required | list of `{ id, host, port }` |
| `pool.maxSocketsPerUpstream` | 128 | sockets per upstream |
| `pool.scheduling` | `lifo` | reuse the most recent idle socket first |
| `timeouts.upstreamResponseTimeoutMs` | 15000 | time to wait for response headers before `504` |
| `timeouts.shutdownGracePeriodMs` | 10000 | time to let requests finish on shutdown |
| `healthCheck.enabled` | `false` | turn on active health checks |
| `healthCheck.unhealthyThreshold` / `healthyThreshold` | 3 / 2 | failures to go down / successes to come back |
| `limits.maxActiveRequests` | 2048 | global limit before `503` |
| `limits.maxActiveRequestsPerUpstream` | 512 | per-upstream limit |
| `limits.maxRequestBodyBytes` | 10 MiB | larger bodies get `413` |
| `limits.maxRetries` | 1 | retries for safe methods |

All options and their validation are in
[src/config/validator.ts](src/config/validator.ts).

## Results

All numbers come from one MacBook (Apple M1 Pro, Node.js 22). The client, the
proxy and the upstreams all ran on the same machine over loopback, so they
shared the CPU. The runs are short (300 to 1,500 requests after a warm-up), so
treat them as rough comparisons, not as capacity numbers.

### Proxy overhead (`npm run benchmark`)

| Scenario | Requests | Concurrency | Req/s | p50 | p99 |
|---|---:|---:|---:|---:|---:|
| Direct to upstream, 1 KiB | 1,500 | 24 | 12,280 | 1.81 ms | 4.31 ms |
| Through proxy, 1 upstream | 1,500 | 24 | 4,719 | 4.80 ms | 10.40 ms |
| Through proxy, 3 upstreams, round-robin | 1,500 | 24 | 5,805 | 3.99 ms | 6.09 ms |
| Through proxy, 3 upstreams, least-connections | 1,500 | 24 | 4,984 | 3.63 ms | 23.03 ms |
| Through proxy, 256 KiB responses | 300 | 12 | 1,694 | 6.57 ms | 12.46 ms |

The proxy adds about 2 ms at the median here. That is expected: every request
is parsed and written one more time in each direction, on a single thread.

### Experiment 1: blocking the event loop

80 proxied requests, with a CPU-heavy hash (`pbkdf2Sync`) every 20 requests.

| Scenario | p95 | p99 | Event loop delay p99 |
|---|---:|---:|---:|
| No CPU work | 11.15 ms | 11.17 ms | 11.49 ms |
| CPU work on the main thread | 30.33 ms | 31.93 ms | 29.75 ms |
| Same work in a worker thread | 10.13 ms | 10.47 ms | 11.40 ms |

CPU work on the main thread delays every other request, not just the one that
caused it. Moving it to `worker_threads` brings latency back to normal.

### Experiment 2: streaming vs buffering

Four slow clients each download a 6 MiB response.

| Proxy | Extra peak memory |
|---|---:|
| Buffers the whole body (`Buffer.concat`) | 58.37 MiB |
| Streams with backpressure | 11.95 MiB |

### Experiment 3: keep-alive vs new connections

240 proxied requests at concurrency 6.

| Upstream connections | TCP handshakes | Req/s | p99 |
|---|---:|---:|---:|
| New connection per request | 240 | 1,681 | 20.04 ms |
| Keep-alive pool | 5 | 3,494 | 3.71 ms |

## Two problems I hit

**Clean `502` while a client is uploading.** I first used
`stream.pipeline(req, upstreamReq)` for the request body. When the upstream
refused the connection during a `POST`, `pipeline` destroyed `req`, which
destroys the client's socket, so the client got a connection reset instead of
a `502`. Now the request body uses `req.pipe(upstreamReq)`, and on failure the
proxy unpipes and drains `req` so the `502` can be sent. The response
direction still uses `pipeline`, because once headers are sent, a broken
upstream should cut the client connection rather than send a truncated body
that looks complete.

**Detecting a client that leaves during a `GET`.** For a request with no body,
`req` ends as soon as the headers are parsed, so its events say nothing about
the client leaving. The proxy listens for `res` `close` and checks
`res.writableEnded` to know when to abort the upstream request.

## Limitations

- HTTP/1.1 only: no TLS, no HTTP/2, no WebSockets.
- One process, so one CPU core.
- Upstreams are fixed at startup.
- The benchmarks are short local runs. Longer runs with `wrk` or `autocannon`,
  with nginx as a baseline, would give more trustworthy numbers.
