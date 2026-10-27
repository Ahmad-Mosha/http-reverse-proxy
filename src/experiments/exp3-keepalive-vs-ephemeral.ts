import { Agent, createServer, request as httpRequest } from 'node:http';
import { performance } from 'node:perf_hooks';
import { fileURLToPath } from 'node:url';
import { LatencyRingBuffer } from '../observability/ring-buffer.js';
import { ProxyServer } from '../proxy/proxy-server.js';

export interface KeepAliveExperimentResult {
  poolMode: 'ephemeral-no-keepalive' | 'pooled-keepalive-lifo';
  totalRequests: number;
  upstreamTcpHandshakes: number;
  requestsPerSecond: number;
  latencyP50Ms: number;
  latencyP95Ms: number;
  latencyP99Ms: number;
}

async function runPoolMode(
  keepAlive: boolean,
  totalRequests = 240,
  concurrency = 6,
): Promise<KeepAliveExperimentResult> {
  let upstreamTcpHandshakes = 0;

  const upstreamServer = createServer((_req, res) => {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end('{"ok":true}');
  });

  upstreamServer.on('connection', () => {
    upstreamTcpHandshakes += 1;
  });

  const upstreamPort = await new Promise<number>((resolve) => {
    upstreamServer.listen(0, '127.0.0.1', () => {
      const addr = upstreamServer.address();
      resolve(typeof addr === 'object' && addr !== null ? addr.port : 0);
    });
  });

  const proxy = new ProxyServer({
    port: 0,
    upstreams: [{ id: 'u1', host: '127.0.0.1', port: upstreamPort }],
    pool: {
      keepAlive,
      scheduling: 'lifo',
      maxSocketsPerUpstream: 64,
      maxFreeSocketsPerUpstream: 16,
    },
  });
  const { port: proxyPort } = await proxy.listen();

  const clientAgent = new Agent({ keepAlive: true, maxSockets: concurrency });
  const latencies = new LatencyRingBuffer(totalRequests);

  let dispatched = 0;
  const startWall = performance.now();

  const worker = async (): Promise<void> => {
    while (dispatched < totalRequests) {
      dispatched += 1;
      await new Promise<void>((resolve, reject) => {
        const t0 = performance.now();
        const req = httpRequest(
          {
            host: '127.0.0.1',
            port: proxyPort,
            path: '/api/check',
            agent: clientAgent,
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

  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  const elapsedSec = (performance.now() - startWall) / 1000;
  const summary = latencies.summarize();

  clientAgent.destroy();
  await proxy.close();
  await new Promise<void>((resolve) => {
    upstreamServer.closeAllConnections();
    upstreamServer.close(() => resolve());
  });

  return {
    poolMode: keepAlive ? 'pooled-keepalive-lifo' : 'ephemeral-no-keepalive',
    totalRequests,
    upstreamTcpHandshakes,
    requestsPerSecond: Math.round(totalRequests / elapsedSec),
    latencyP50Ms: summary.p50,
    latencyP95Ms: summary.p95,
    latencyP99Ms: summary.p99,
  };
}

export async function runKeepAliveExperiment(): Promise<KeepAliveExperimentResult[]> {
  const ephemeral = await runPoolMode(false);
  const pooled = await runPoolMode(true);
  return [ephemeral, pooled];
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  void runKeepAliveExperiment().then((results) => {
    process.stdout.write(
      '\n=== Experiment 3: Upstream Keep-Alive Pool vs Ephemeral Sockets ===\n',
    );
    console.table(results);
  });
}
