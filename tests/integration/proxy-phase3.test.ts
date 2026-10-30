import assert from 'node:assert/strict';
import { createServer, request as httpRequest } from 'node:http';
import type { IncomingMessage, Server, ServerResponse } from 'node:http';
import { PassThrough } from 'node:stream';
import { afterEach, describe, it } from 'node:test';
import { runEventLoopExperiment } from '../../src/experiments/exp1-event-loop-blocking.js';
import { runStreamingVsBufferingExperiment } from '../../src/experiments/exp2-streaming-vs-buffering.js';
import { runKeepAliveExperiment } from '../../src/experiments/exp3-keepalive-vs-ephemeral.js';
import type { ProxyMetricsSnapshot } from '../../src/observability/metrics.js';
import { LatencyRingBuffer } from '../../src/observability/ring-buffer.js';
import { ProxyServer } from '../../src/proxy/proxy-server.js';

interface TestUpstream {
  id: string;
  port: number;
  server: Server;
  close(): Promise<void>;
}

async function createTestUpstream(
  id: string,
  handler: (req: IncomingMessage, res: ServerResponse) => void,
): Promise<TestUpstream> {
  const server = createServer(handler);
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const addr = server.address();
      if (addr === null || typeof addr !== 'object') {
        reject(new Error('Failed to bind test upstream server'));
        return;
      }
      resolve({
        id,
        port: addr.port,
        server,
        close: () =>
          new Promise<void>((resClose) => {
            server.closeAllConnections();
            server.close(() => resClose());
          }),
      });
    });
  });
}

async function sendGet(
  port: number,
  path: string,
): Promise<{ statusCode: number; body: string }> {
  return new Promise((resolve, reject) => {
    const req = httpRequest({ host: '127.0.0.1', port, method: 'GET', path }, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (c: Buffer) => chunks.push(c));
      res.on('end', () => {
        resolve({
          statusCode: res.statusCode ?? 0,
          body: Buffer.concat(chunks).toString('utf8'),
        });
      });
    });
    req.on('error', reject);
    req.end();
  });
}

describe('Phase 3 — Observability, Metrics, Structured Logs & Runtime Experiments', () => {
  const activeProxies: ProxyServer[] = [];
  const activeUpstreams: TestUpstream[] = [];

  afterEach(async () => {
    while (activeProxies.length > 0) {
      const proxy = activeProxies.pop();
      if (proxy !== undefined) {
        await proxy.close();
      }
    }
    while (activeUpstreams.length > 0) {
      const upstream = activeUpstreams.pop();
      if (upstream !== undefined) {
        await upstream.close();
      }
    }
  });

  it('computes accurate percentiles in LatencyRingBuffer with bounded capacity', () => {
    const ring = new LatencyRingBuffer(100);
    for (let i = 1; i <= 100; i += 1) {
      ring.record(i);
    }

    const summary = ring.summarize();
    assert.equal(summary.count, 100);
    assert.equal(summary.mean, 50.5);
    assert.equal(summary.p50, 50);
    assert.equal(summary.p95, 95);
    assert.equal(summary.p99, 99);
    assert.equal(summary.max, 100);
  });

  it('exposes live operational metrics, health endpoint, and structured NDJSON logs', async () => {
    const u1 = await createTestUpstream('u1', (req, res) => {
      if (req.url === '/fail') {
        res.writeHead(404);
        res.end('not found');
        return;
      }
      res.writeHead(200, { 'content-type': 'text/plain' });
      res.end('ok');
    });
    activeUpstreams.push(u1);

    const logStream = new PassThrough();
    const logLines: string[] = [];
    logStream.on('data', (chunk: Buffer) => {
      for (const line of chunk.toString('utf8').split('\n')) {
        if (line.trim().length > 0) {
          logLines.push(line);
        }
      }
    });

    const proxy = new ProxyServer(
      {
        port: 0,
        upstreams: [{ id: 'u1', host: '127.0.0.1', port: u1.port }],
        observability: {
          logLevel: 'info',
          metricsPath: '/__proxy/metrics',
          healthPath: '/__proxy/healthz',
        },
      },
      logStream,
    );
    activeProxies.push(proxy);
    const { port: proxyPort } = await proxy.listen();

    await sendGet(proxyPort, '/hello');
    await sendGet(proxyPort, '/hello');
    await sendGet(proxyPort, '/fail');

    const healthRes = await sendGet(proxyPort, '/__proxy/healthz');
    assert.equal(healthRes.statusCode, 200);
    const healthJson = JSON.parse(healthRes.body) as {
      status: string;
      healthyUpstreams: number;
    };
    assert.equal(healthJson.status, 'healthy');
    assert.equal(healthJson.healthyUpstreams, 1);

    const metricsRes = await sendGet(proxyPort, '/__proxy/metrics');
    assert.equal(metricsRes.statusCode, 200);
    const snapshot = JSON.parse(metricsRes.body) as ProxyMetricsSnapshot;

    assert.equal(snapshot.requests.total, 3);
    assert.equal(snapshot.requests.byStatusClass['2xx'], 2);
    assert.equal(snapshot.requests.byStatusClass['4xx'], 1);
    assert.equal(snapshot.latencyMs.requestDuration.count, 3);
    assert.equal(snapshot.latencyMs.upstreamResponse.count, 3);
    assert.ok(snapshot.memory.rssBytes > 0);
    assert.ok(snapshot.eventLoop.delayMaxMs >= 0);

    assert.ok(logLines.length >= 4);
    const parsedLogs = logLines.map(
      (l) => JSON.parse(l) as { event: string; statusCode?: number },
    );
    assert.equal(parsedLogs[0]?.event, 'proxy.listening');
    assert.equal(parsedLogs.filter((l) => l.event === 'request.completed').length, 3);
  });

  it('verifies Experiment 1 (Main-Thread CPU Blocking vs Worker Thread Offload)', async () => {
    const results = await runEventLoopExperiment();
    const baseline = results.find((r) => r.scenario === 'baseline-io');
    const blocked = results.find((r) => r.scenario === 'main-thread-cpu-block');
    const offloaded = results.find((r) => r.scenario === 'worker-thread-offload');

    assert.ok(baseline && blocked && offloaded);
    assert.equal(baseline.requestsCompleted, 80);
    assert.equal(blocked.requestsCompleted, 80);
    assert.equal(offloaded.requestsCompleted, 80);

    // Main-thread CPU blocking spikes tail latency compared to baseline and worker offload
    assert.ok(
      blocked.latencyMaxMs > baseline.latencyMaxMs,
      `Expected blocked max latency (${blocked.latencyMaxMs}ms) > baseline (${baseline.latencyMaxMs}ms)`,
    );
    assert.ok(
      blocked.latencyP95Ms > offloaded.latencyP95Ms,
      `Expected blocked p95 latency (${blocked.latencyP95Ms}ms) > worker offloaded (${offloaded.latencyP95Ms}ms)`,
    );
  });

  it('verifies Experiment 2 (Streaming Backpressure vs In-Memory Buffering)', async () => {
    const results = await runStreamingVsBufferingExperiment(3, 64);
    const buffered = results.find((r) => r.mode === 'naive-buffered-proxy');
    const streaming = results.find((r) => r.mode === 'streaming-proxy');

    assert.ok(buffered && streaming);
    assert.ok(
      streaming.backpressureDrainEvents > 0,
      'Expected streaming proxy to record backpressure drain events',
    );
    assert.ok(
      buffered.peakBufferMemoryDeltaMiB > streaming.peakBufferMemoryDeltaMiB,
      `Expected buffered peak memory delta (${buffered.peakBufferMemoryDeltaMiB} MiB) > streaming (${streaming.peakBufferMemoryDeltaMiB} MiB)`,
    );
  });

  it('verifies Experiment 3 (Keep-Alive Pool vs Ephemeral Sockets)', async () => {
    const results = await runKeepAliveExperiment();
    const ephemeral = results.find((r) => r.poolMode === 'ephemeral-no-keepalive');
    const pooled = results.find((r) => r.poolMode === 'pooled-keepalive-lifo');

    assert.ok(ephemeral && pooled);
    assert.equal(ephemeral.upstreamTcpHandshakes, ephemeral.totalRequests);
    assert.ok(
      pooled.upstreamTcpHandshakes <= 6,
      `Expected pooled TCP handshakes (${pooled.upstreamTcpHandshakes}) <= concurrency (6)`,
    );
  });
});
