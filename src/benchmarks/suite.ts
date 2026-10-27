import { Agent, createServer, request as httpRequest } from 'node:http';
import type { Server } from 'node:http';
import os from 'node:os';
import { performance } from 'node:perf_hooks';
import { fileURLToPath } from 'node:url';
import { LatencyRingBuffer } from '../observability/ring-buffer.js';
import { ProxyServer } from '../proxy/proxy-server.js';
import type { LoadBalancingStrategy } from '../types/config.js';

export interface BenchmarkEnvironment {
  nodeVersion: string;
  platform: string;
  arch: string;
  cpuModel: string;
  cpuCores: number;
  totalMemoryGiB: number;
  timestamp: string;
}

export interface BenchmarkRow {
  benchmark: string;
  requests: number;
  concurrency: number;
  payloadBytes: number;
  requestsPerSec: number;
  throughputMiBPerSec: number;
  meanMs: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  eventLoopDelayP99Ms: number;
  rssMiB: number;
}

export interface BenchmarkReport {
  environment: BenchmarkEnvironment;
  results: BenchmarkRow[];
}

export function getBenchmarkEnvironment(): BenchmarkEnvironment {
  const cpus = os.cpus();
  return {
    nodeVersion: process.version,
    platform: process.platform,
    arch: process.arch,
    cpuModel: cpus[0]?.model ?? 'Unknown CPU',
    cpuCores: cpus.length,
    totalMemoryGiB: Math.round((os.totalmem() / 1024 ** 3) * 10) / 10,
    timestamp: new Date().toISOString(),
  };
}

async function startPayloadUpstream(payload: Buffer): Promise<{
  port: number;
  server: Server;
}> {
  const contentLength = String(payload.byteLength);
  const server = createServer((_req, res) => {
    res.writeHead(200, {
      'content-type': 'application/octet-stream',
      'content-length': contentLength,
    });
    res.end(payload);
  });

  server.on('connection', (socket) => {
    socket.setNoDelay(true);
  });

  const port = await new Promise<number>((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const addr = server.address();
      resolve(typeof addr === 'object' && addr !== null ? addr.port : 0);
    });
  });

  return { port, server };
}

async function driveLoad(params: {
  targetPort: number;
  requests: number;
  concurrency: number;
  agent: Agent;
}): Promise<{
  elapsedSec: number;
  latencies: LatencyRingBuffer;
}> {
  const { agent } = params;
  const latencies = new LatencyRingBuffer(params.requests);
  let dispatched = 0;

  const start = performance.now();

  const worker = async (): Promise<void> => {
    while (dispatched < params.requests) {
      dispatched += 1;
      await new Promise<void>((resolve, reject) => {
        const t0 = performance.now();
        const req = httpRequest(
          {
            host: '127.0.0.1',
            port: params.targetPort,
            method: 'GET',
            path: '/bench',
            agent,
          },
          (res) => {
            res.resume();
            res.on('end', () => {
              latencies.record(performance.now() - t0);
              resolve();
            });
          },
        );
        req.on('error', reject);
        req.end();
      });
    }
  };

  await Promise.all(Array.from({ length: params.concurrency }, () => worker()));
  const elapsedSec = (performance.now() - start) / 1000;

  return { elapsedSec, latencies };
}

async function runProxyBenchmarkCase(options: {
  name: string;
  upstreamCount: number;
  strategy: LoadBalancingStrategy;
  payloadBytes: number;
  warmupRequests: number;
  requests: number;
  concurrency: number;
}): Promise<BenchmarkRow> {
  const payload = Buffer.alloc(options.payloadBytes, 0x78);
  const upstreams = await Promise.all(
    Array.from({ length: options.upstreamCount }, () => startPayloadUpstream(payload)),
  );

  let targetPort = upstreams[0]?.port ?? 0;
  let proxy: ProxyServer | null = null;

  if (options.upstreamCount > 0 && options.name !== 'direct-upstream-1kib') {
    proxy = new ProxyServer({
      port: 0,
      strategy: options.strategy,
      upstreams: upstreams.map((u, idx) => ({
        id: `u-${idx + 1}`,
        host: '127.0.0.1',
        port: u.port,
      })),
    });
    const bound = await proxy.listen();
    targetPort = bound.port;
  }

  const clientAgent = new Agent({
    keepAlive: true,
    maxSockets: options.concurrency,
    scheduling: 'lifo',
  });

  // Warm-up phase at full target concurrency so both client and upstream Keep-Alive pools are warm
  await driveLoad({
    targetPort,
    requests: options.warmupRequests,
    concurrency: options.concurrency,
    agent: clientAgent,
  });
  proxy?.resetEventLoopStats();

  // Measured phase
  const { elapsedSec, latencies } = await driveLoad({
    targetPort,
    requests: options.requests,
    concurrency: options.concurrency,
    agent: clientAgent,
  });

  clientAgent.destroy();

  const summary = latencies.summarize();
  const snapshot = proxy?.getMetricsSnapshot();
  const rssMiB = Math.round((process.memoryUsage().rss / (1024 * 1024)) * 10) / 10;

  if (proxy !== null) {
    await proxy.close();
  }
  await Promise.all(
    upstreams.map(
      (u) =>
        new Promise<void>((resolve) => {
          u.server.closeAllConnections();
          u.server.close(() => resolve());
        }),
    ),
  );

  const totalMiB = (options.requests * options.payloadBytes) / (1024 * 1024);

  return {
    benchmark: options.name,
    requests: options.requests,
    concurrency: options.concurrency,
    payloadBytes: options.payloadBytes,
    requestsPerSec: Math.round(options.requests / elapsedSec),
    throughputMiBPerSec: Math.round((totalMiB / elapsedSec) * 100) / 100,
    meanMs: summary.mean,
    p50Ms: summary.p50,
    p95Ms: summary.p95,
    p99Ms: summary.p99,
    eventLoopDelayP99Ms: snapshot?.eventLoop.delayP99Ms ?? 0,
    rssMiB,
  };
}

export async function runBenchmarkSuite(): Promise<BenchmarkReport> {
  const environment = getBenchmarkEnvironment();
  const results: BenchmarkRow[] = [];

  results.push(
    await runProxyBenchmarkCase({
      name: 'direct-upstream-1kib',
      upstreamCount: 1,
      strategy: 'round-robin',
      payloadBytes: 1024,
      warmupRequests: 200,
      requests: 1500,
      concurrency: 24,
    }),
  );

  results.push(
    await runProxyBenchmarkCase({
      name: 'relay-edge-1-upstream-1kib',
      upstreamCount: 1,
      strategy: 'round-robin',
      payloadBytes: 1024,
      warmupRequests: 200,
      requests: 1500,
      concurrency: 24,
    }),
  );

  results.push(
    await runProxyBenchmarkCase({
      name: 'relay-edge-3-upstreams-rr-1kib',
      upstreamCount: 3,
      strategy: 'round-robin',
      payloadBytes: 1024,
      warmupRequests: 200,
      requests: 1500,
      concurrency: 24,
    }),
  );

  results.push(
    await runProxyBenchmarkCase({
      name: 'relay-edge-3-upstreams-lc-1kib',
      upstreamCount: 3,
      strategy: 'least-connections',
      payloadBytes: 1024,
      warmupRequests: 200,
      requests: 1500,
      concurrency: 24,
    }),
  );

  results.push(
    await runProxyBenchmarkCase({
      name: 'relay-edge-streaming-256kib',
      upstreamCount: 2,
      strategy: 'round-robin',
      payloadBytes: 256 * 1024,
      warmupRequests: 40,
      requests: 300,
      concurrency: 12,
    }),
  );

  return { environment, results };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  void runBenchmarkSuite().then((report) => {
    process.stdout.write('\n=== RelayEdge Benchmark Suite ===\n');
    process.stdout.write(
      `Environment: Node ${report.environment.nodeVersion} | ${report.environment.cpuModel} (${report.environment.cpuCores} cores, ${report.environment.totalMemoryGiB} GiB RAM) | ${report.environment.platform}/${report.environment.arch}\n\n`,
    );
    console.table(report.results);
  });
}
