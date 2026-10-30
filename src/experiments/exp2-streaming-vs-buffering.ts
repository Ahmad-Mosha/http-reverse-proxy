import { createServer, request as httpRequest } from 'node:http';
import { performance } from 'node:perf_hooks';
import { fileURLToPath } from 'node:url';
import { ProxyServer } from '../proxy/proxy-server.js';

export interface StreamingExperimentResult {
  mode: 'naive-buffered-proxy' | 'streaming-proxy';
  concurrentClients: number;
  payloadPerClientMiB: number;
  peakBufferMemoryDeltaMiB: number;
  durationMs: number;
  backpressureDrainEvents: number;
}

function getBufferAndHeapBytes(): number {
  const m = process.memoryUsage();
  return m.heapUsed + m.external;
}

async function consumeSlowly(port: number): Promise<number> {
  return new Promise((resolve, reject) => {
    let receivedBytes = 0;
    const req = httpRequest(
      {
        host: '127.0.0.1',
        port,
        method: 'GET',
        path: '/download',
      },
      (res) => {
        res.pause();
        let delays = 8;
        const readLoop = (): void => {
          let chunk: Buffer | null;
          while ((chunk = res.read() as Buffer | null) !== null) {
            receivedBytes += chunk.byteLength;
            if (delays > 0) {
              delays -= 1;
              setTimeout(readLoop, 12);
              return;
            }
          }
        };
        res.on('readable', readLoop);
        res.on('end', () => resolve(receivedBytes));
        res.on('error', reject);
      },
    );
    req.on('error', reject);
    req.end();
  });
}

export async function runStreamingVsBufferingExperiment(
  concurrentClients = 4,
  chunksPerRequest = 96, // 96 * 64 KiB = 6 MiB per client (24 MiB total concurrent)
): Promise<StreamingExperimentResult[]> {
  const chunkSize = 64 * 1024;
  const sharedChunk = Buffer.alloc(chunkSize, 0x61);
  const payloadMiB =
    Math.round(((chunksPerRequest * chunkSize) / (1024 * 1024)) * 100) / 100;

  const upstreamServer = createServer((_req, res) => {
    res.writeHead(200, {
      'content-type': 'application/octet-stream',
      'content-length': String(chunksPerRequest * chunkSize),
    });
    let sent = 0;
    const pump = (): void => {
      while (sent < chunksPerRequest) {
        sent += 1;
        if (!res.write(sharedChunk)) {
          res.once('drain', pump);
          return;
        }
      }
      res.end();
    };
    pump();
  });

  const upstreamPort = await new Promise<number>((resolve) => {
    upstreamServer.listen(0, '127.0.0.1', () => {
      const addr = upstreamServer.address();
      resolve(typeof addr === 'object' && addr !== null ? addr.port : 0);
    });
  });

  // 1. Naive Buffered Proxy (anti-pattern: buffers entire upstream body before responding)
  const bufferedProxy = createServer((_clientReq, clientRes) => {
    const upReq = httpRequest(
      {
        host: '127.0.0.1',
        port: upstreamPort,
        method: 'GET',
        path: '/download',
      },
      (upRes) => {
        const chunks: Buffer[] = [];
        upRes.on('data', (c: Buffer) => chunks.push(c));
        upRes.on('end', () => {
          const fullBody = Buffer.concat(chunks);
          clientRes.writeHead(200, { 'content-length': String(fullBody.byteLength) });
          clientRes.end(fullBody);
        });
      },
    );
    upReq.end();
  });

  const bufferedPort = await new Promise<number>((resolve) => {
    bufferedProxy.listen(0, '127.0.0.1', () => {
      const addr = bufferedProxy.address();
      resolve(typeof addr === 'object' && addr !== null ? addr.port : 0);
    });
  });

  const baseMemBuffered = getBufferAndHeapBytes();
  let peakMemBuffered = baseMemBuffered;
  const sampleTimer1 = setInterval(() => {
    const cur = getBufferAndHeapBytes();
    if (cur > peakMemBuffered) {
      peakMemBuffered = cur;
    }
  }, 4);

  const startBuffered = performance.now();
  await Promise.all(
    Array.from({ length: concurrentClients }, () => consumeSlowly(bufferedPort)),
  );
  const bufferedDurationMs = Math.round((performance.now() - startBuffered) * 100) / 100;
  clearInterval(sampleTimer1);

  await new Promise<void>((resolve) => {
    bufferedProxy.closeAllConnections();
    bufferedProxy.close(() => resolve());
  });

  // 2. Streaming proxy with 16 KiB highWaterMark
  const streamingProxy = new ProxyServer({
    port: 0,
    upstreams: [{ id: 'u1', host: '127.0.0.1', port: upstreamPort }],
    stream: {
      highWaterMarkBytes: 16 * 1024,
    },
  });
  const { port: streamingPort } = await streamingProxy.listen();

  const baseMemStreaming = getBufferAndHeapBytes();
  let peakMemStreaming = baseMemStreaming;
  const sampleTimer2 = setInterval(() => {
    const cur = getBufferAndHeapBytes();
    if (cur > peakMemStreaming) {
      peakMemStreaming = cur;
    }
  }, 4);

  const startStreaming = performance.now();
  await Promise.all(
    Array.from({ length: concurrentClients }, () => consumeSlowly(streamingPort)),
  );
  const streamingDurationMs =
    Math.round((performance.now() - startStreaming) * 100) / 100;
  clearInterval(sampleTimer2);

  const drainEvents = streamingProxy.getBackpressureMetrics().clientPauseCount;

  await streamingProxy.close();
  await new Promise<void>((resolve) => {
    upstreamServer.closeAllConnections();
    upstreamServer.close(() => resolve());
  });

  const toMiB = (bytes: number): number =>
    Math.max(0, Math.round((bytes / (1024 * 1024)) * 100) / 100);

  return [
    {
      mode: 'naive-buffered-proxy',
      concurrentClients,
      payloadPerClientMiB: payloadMiB,
      peakBufferMemoryDeltaMiB: toMiB(peakMemBuffered - baseMemBuffered),
      durationMs: bufferedDurationMs,
      backpressureDrainEvents: 0,
    },
    {
      mode: 'streaming-proxy',
      concurrentClients,
      payloadPerClientMiB: payloadMiB,
      peakBufferMemoryDeltaMiB: toMiB(peakMemStreaming - baseMemStreaming),
      durationMs: streamingDurationMs,
      backpressureDrainEvents: drainEvents,
    },
  ];
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  void runStreamingVsBufferingExperiment().then((results) => {
    process.stdout.write(
      '\n=== Experiment 2: Streaming Backpressure vs In-Memory Buffering ===\n',
    );
    console.table(results);
  });
}
