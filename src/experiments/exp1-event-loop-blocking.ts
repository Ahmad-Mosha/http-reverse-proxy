import { pbkdf2Sync } from 'node:crypto';
import { createServer, request as httpRequest, Agent } from 'node:http';
import { performance } from 'node:perf_hooks';
import { fileURLToPath } from 'node:url';
import { Worker } from 'node:worker_threads';
import { LatencyRingBuffer } from '../observability/ring-buffer.js';
import { ProxyServer } from '../proxy/proxy-server.js';

export interface EventLoopExperimentResult {
  scenario: 'baseline-io' | 'main-thread-cpu-block' | 'worker-thread-offload';
  requestsCompleted: number;
  latencyP50Ms: number;
  latencyP95Ms: number;
  latencyP99Ms: number;
  latencyMaxMs: number;
  eventLoopDelayP99Ms: number;
  eventLoopDelayMaxMs: number;
  eventLoopUtilization: number;
}

function runCpuTaskOnMainThread(iterations: number): void {
  pbkdf2Sync('cpu-experiment', 'salt-value', iterations, 64, 'sha512');
}

function runCpuTaskInWorker(iterations: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const workerCode = `
      const { parentPort, workerData } = require('node:worker_threads');
      const { pbkdf2Sync } = require('node:crypto');
      pbkdf2Sync('cpu-experiment', 'salt-value', workerData.iterations, 64, 'sha512');
      parentPort.postMessage('done');
    `;
    const worker = new Worker(workerCode, {
      eval: true,
      workerData: { iterations },
    });
    worker.once('message', () => {
      void worker.terminate().then(() => resolve());
    });
    worker.once('error', reject);
  });
}

async function runScenario(
  scenario: EventLoopExperimentResult['scenario'],
  totalRequests = 80,
  concurrency = 8,
  cpuIterations = 60_000,
): Promise<EventLoopExperimentResult> {
  const upstreamServer = createServer((_req, res) => {
    setTimeout(() => {
      res.writeHead(200, { 'content-type': 'text/plain' });
      res.end('ok');
    }, 2);
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
  });
  const { port: proxyPort } = await proxy.listen();
  proxy.resetEventLoopStats();

  const clientAgent = new Agent({ keepAlive: true, maxSockets: concurrency });
  const latencies = new LatencyRingBuffer(totalRequests);

  let dispatched = 0;
  const cpuPromises: Promise<void>[] = [];

  const sendOne = (): Promise<void> =>
    new Promise((resolve, reject) => {
      const start = performance.now();
      const req = httpRequest(
        {
          host: '127.0.0.1',
          port: proxyPort,
          path: '/ping',
          agent: clientAgent,
        },
        (res) => {
          res.resume();
          res.on('end', () => {
            latencies.record(performance.now() - start);
            resolve();
          });
        },
      );
      req.on('error', reject);
      req.end();
    });

  const workerLoop = async (): Promise<void> => {
    while (dispatched < totalRequests) {
      const current = dispatched;
      dispatched += 1;

      if (current > 0 && current % 20 === 0) {
        if (scenario === 'main-thread-cpu-block') {
          runCpuTaskOnMainThread(cpuIterations);
        } else if (scenario === 'worker-thread-offload') {
          cpuPromises.push(runCpuTaskInWorker(cpuIterations));
        }
      }

      await sendOne();
    }
  };

  await Promise.all(Array.from({ length: concurrency }, () => workerLoop()));
  await Promise.all(cpuPromises);

  // Allow monitorEventLoopDelay histogram to sample final tick
  await new Promise((r) => setTimeout(r, 25));

  const snapshot = proxy.getMetricsSnapshot();
  const summary = latencies.summarize();

  clientAgent.destroy();
  await proxy.close();
  await new Promise<void>((resolve) => {
    upstreamServer.closeAllConnections();
    upstreamServer.close(() => resolve());
  });

  return {
    scenario,
    requestsCompleted: summary.count,
    latencyP50Ms: summary.p50,
    latencyP95Ms: summary.p95,
    latencyP99Ms: summary.p99,
    latencyMaxMs: summary.max,
    eventLoopDelayP99Ms: snapshot.eventLoop.delayP99Ms,
    eventLoopDelayMaxMs: snapshot.eventLoop.delayMaxMs,
    eventLoopUtilization: snapshot.eventLoop.utilization,
  };
}

export async function runEventLoopExperiment(): Promise<EventLoopExperimentResult[]> {
  // Warm up V8 JIT and HTTP parsers so the first measured scenario isn't penalized by cold compilation
  await runScenario('baseline-io', 24, 4, 1_000);

  const baseline = await runScenario('baseline-io', 80, 8, 90_000);
  const blocked = await runScenario('main-thread-cpu-block', 80, 8, 90_000);
  const offloaded = await runScenario('worker-thread-offload', 80, 8, 90_000);
  return [baseline, blocked, offloaded];
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  void runEventLoopExperiment().then((results) => {
    process.stdout.write(
      '\n=== Experiment 1: Event Loop Blocking vs Worker Thread ===\n',
    );
    console.table(results);
  });
}
