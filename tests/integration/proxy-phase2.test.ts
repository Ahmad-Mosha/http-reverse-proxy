import assert from 'node:assert/strict';
import { createServer, request as httpRequest } from 'node:http';
import type { IncomingMessage, Server, ServerResponse } from 'node:http';
import { afterEach, describe, it } from 'node:test';
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

interface HttpResponseResult {
  statusCode: number;
  headers: IncomingMessage['headers'];
  body: Buffer;
}

async function sendRequest(options: {
  port: number;
  method?: string;
  path?: string;
  headers?: Record<string, string>;
  body?: Buffer | string;
}): Promise<HttpResponseResult> {
  return new Promise((resolve, reject) => {
    const req = httpRequest(
      {
        host: '127.0.0.1',
        port: options.port,
        method: options.method ?? 'GET',
        path: options.path ?? '/',
        headers: options.headers,
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (chunk: Buffer) => chunks.push(chunk));
        res.on('end', () => {
          resolve({
            statusCode: res.statusCode ?? 0,
            headers: res.headers,
            body: Buffer.concat(chunks),
          });
        });
        res.on('error', reject);
      },
    );

    req.on('error', reject);
    if (options.body !== undefined) {
      req.write(options.body);
    }
    req.end();
  });
}

describe('Phase 2 Integration — Reliability, Health Checks, Limits & Graceful Shutdown', () => {
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

  it('transitions upstream health via active probes with hysteresis thresholds', async () => {
    let u1Healthy = true;

    const u1 = await createTestUpstream('u1', (req, res) => {
      if (req.url === '/healthz') {
        res.writeHead(u1Healthy ? 200 : 503);
        res.end(u1Healthy ? 'ok' : 'failing');
        return;
      }
      res.writeHead(200);
      res.end('u1');
    });

    const u2 = await createTestUpstream('u2', (req, res) => {
      if (req.url === '/healthz') {
        res.writeHead(200);
        res.end('ok');
        return;
      }
      res.writeHead(200);
      res.end('u2');
    });
    activeUpstreams.push(u1, u2);

    const proxy = new ProxyServer({
      port: 0,
      strategy: 'round-robin',
      upstreams: [
        { id: 'u1', host: '127.0.0.1', port: u1.port },
        { id: 'u2', host: '127.0.0.1', port: u2.port },
      ],
      healthCheck: {
        enabled: false, // Trigger cycles deterministically in test
        path: '/healthz',
        intervalMs: 10_000,
        timeoutMs: 500,
        unhealthyThreshold: 2,
        healthyThreshold: 2,
      },
    });
    activeProxies.push(proxy);
    const { port: proxyPort } = await proxy.listen();

    // Flip u1 health endpoint to fail
    u1Healthy = false;

    // 1st failed probe: below unhealthyThreshold (2), so u1 remains healthy
    await proxy.runHealthCheckCycle();
    assert.equal(proxy.getPool().getUpstreamById('u1')?.healthy, true);

    // 2nd consecutive failed probe: reaches unhealthyThreshold (2), u1 becomes unhealthy
    await proxy.runHealthCheckCycle();
    assert.equal(proxy.getPool().getUpstreamById('u1')?.healthy, false);

    // All traffic now routes only to u2
    for (let i = 0; i < 4; i += 1) {
      const res = await sendRequest({ port: proxyPort, path: '/work' });
      assert.equal(res.body.toString('utf8'), 'u2');
    }

    // u1 recovers
    u1Healthy = true;

    // 1st successful probe: below healthyThreshold (2), still unhealthy
    await proxy.runHealthCheckCycle();
    assert.equal(proxy.getPool().getUpstreamById('u1')?.healthy, false);

    // 2nd consecutive successful probe: reaches healthyThreshold (2), u1 is healthy again
    await proxy.runHealthCheckCycle();
    assert.equal(proxy.getPool().getUpstreamById('u1')?.healthy, true);

    const afterRecovery = [
      (await sendRequest({ port: proxyPort, path: '/work' })).body.toString('utf8'),
      (await sendRequest({ port: proxyPort, path: '/work' })).body.toString('utf8'),
    ];
    assert.deepEqual(new Set(afterRecovery), new Set(['u1', 'u2']));
  });

  it('retries failed GET requests on a healthy upstream but refuses to retry non-idempotent POST requests', async () => {
    const u1 = await createTestUpstream('u1', (_req, res) => {
      res.writeHead(200);
      res.end('u1');
    });
    const u2 = await createTestUpstream('u2', (_req, res) => {
      res.writeHead(200);
      res.end('u2');
    });
    activeUpstreams.push(u2);

    // Shut down u1 so its port refuses connections
    const deadU1Port = u1.port;
    await u1.close();

    const proxy = new ProxyServer({
      port: 0,
      strategy: 'round-robin',
      upstreams: [
        { id: 'u1', host: '127.0.0.1', port: deadU1Port },
        { id: 'u2', host: '127.0.0.1', port: u2.port },
      ],
      healthCheck: {
        unhealthyThreshold: 3,
      },
      limits: {
        maxRetries: 1,
      },
    });
    activeProxies.push(proxy);
    const { port: proxyPort } = await proxy.listen();

    // 1st request is a GET that lands on dead u1 first, then transparently retries on u2
    const getRes = await sendRequest({
      port: proxyPort,
      method: 'GET',
      path: '/items',
    });
    assert.equal(getRes.statusCode, 200);
    assert.equal(getRes.body.toString('utf8'), 'u2');
    assert.equal(proxy.getPool().getUpstreamById('u1')?.totalErrors, 1);

    // Next cursor in round-robin is u1 again (since cursor advanced twice: u1 -> u2).
    // Sending a non-idempotent POST landing on u1 must NOT be retried!
    const postRes = await sendRequest({
      port: proxyPort,
      method: 'POST',
      path: '/orders',
      body: '{"item":"gpu"}',
    });
    assert.equal(postRes.statusCode, 502);
  });

  it('sheds load with 503 when global or per-upstream concurrency limits are reached', async () => {
    let notifyInFlight!: () => void;
    const inFlightArrived = new Promise<void>((resolve) => {
      notifyInFlight = resolve;
    });
    let releaseInFlight!: () => void;
    const inFlightRelease = new Promise<void>((resolve) => {
      releaseInFlight = resolve;
    });

    const u1 = await createTestUpstream('u1', (req, res) => {
      if (req.url === '/hold') {
        notifyInFlight();
        void inFlightRelease.then(() => {
          res.writeHead(200);
          res.end('released');
        });
        return;
      }
      res.writeHead(200);
      res.end('ok');
    });
    activeUpstreams.push(u1);

    const proxy = new ProxyServer({
      port: 0,
      upstreams: [{ id: 'u1', host: '127.0.0.1', port: u1.port }],
      limits: {
        maxActiveRequests: 1,
        maxActiveRequestsPerUpstream: 1,
      },
    });
    activeProxies.push(proxy);
    const { port: proxyPort } = await proxy.listen();

    const firstReq = sendRequest({ port: proxyPort, path: '/hold' });
    await inFlightArrived;

    // 2nd concurrent request exceeds maxActiveRequests (1) -> 503 OVERLOADED
    const overloadedRes = await sendRequest({ port: proxyPort, path: '/extra' });
    assert.equal(overloadedRes.statusCode, 503);
    const parsed = JSON.parse(overloadedRes.body.toString('utf8')) as {
      error: { code: string };
    };
    assert.equal(parsed.error.code, 'OVERLOADED');

    releaseInFlight();
    const firstResult = await firstReq;
    assert.equal(firstResult.statusCode, 200);

    // Once released, subsequent requests succeed
    const afterRelease = await sendRequest({ port: proxyPort, path: '/extra' });
    assert.equal(afterRelease.statusCode, 200);
  });

  it('rejects oversized request bodies (both Content-Length and chunked stream) with 413 Payload Too Large', async () => {
    const u1 = await createTestUpstream('u1', (req, res) => {
      req.resume();
      req.on('end', () => {
        res.writeHead(200);
        res.end('ok');
      });
    });
    activeUpstreams.push(u1);

    const proxy = new ProxyServer({
      port: 0,
      upstreams: [{ id: 'u1', host: '127.0.0.1', port: u1.port }],
      limits: {
        maxRequestBodyBytes: 64,
      },
    });
    activeProxies.push(proxy);
    const { port: proxyPort } = await proxy.listen();

    // 1. Content-Length header exceeds 64 bytes
    const clRes = await sendRequest({
      port: proxyPort,
      method: 'POST',
      path: '/upload',
      body: 'x'.repeat(128),
    });
    assert.equal(clRes.statusCode, 413);

    // 2. Chunked streaming body exceeding 64 bytes without upfront Content-Length
    const chunkedStatus = await new Promise<number>((resolve, reject) => {
      const req = httpRequest(
        {
          host: '127.0.0.1',
          port: proxyPort,
          method: 'POST',
          path: '/upload-stream',
          headers: {
            'transfer-encoding': 'chunked',
          },
        },
        (res) => {
          res.resume();
          res.on('end', () => resolve(res.statusCode ?? 0));
        },
      );
      req.on('error', reject);
      req.write(Buffer.alloc(40, 'a'));
      req.write(Buffer.alloc(40, 'b'));
      req.end();
    });
    assert.equal(chunkedStatus, 413);
  });

  it('drains in-flight requests cleanly during graceful shutdown and rejects new requests', async () => {
    let notifyUpstreamStarted!: () => void;
    const upstreamStarted = new Promise<void>((resolve) => {
      notifyUpstreamStarted = resolve;
    });
    let completeUpstream!: () => void;
    const upstreamComplete = new Promise<void>((resolve) => {
      completeUpstream = resolve;
    });

    const u1 = await createTestUpstream('u1', (_req, res) => {
      notifyUpstreamStarted();
      void upstreamComplete.then(() => {
        res.writeHead(200);
        res.end('finished-before-shutdown');
      });
    });
    activeUpstreams.push(u1);

    const proxy = new ProxyServer({
      port: 0,
      upstreams: [{ id: 'u1', host: '127.0.0.1', port: u1.port }],
      timeouts: {
        shutdownGracePeriodMs: 2_000,
      },
    });
    activeProxies.push(proxy);
    const { port: proxyPort } = await proxy.listen();

    const inFlightPromise = sendRequest({ port: proxyPort, path: '/in-flight' });
    await upstreamStarted;

    // Trigger graceful shutdown while request is in-flight
    const closePromise = proxy.close();
    assert.equal(proxy.isDraining(), true);

    // Allow in-flight request to finish after 30ms
    setTimeout(() => {
      completeUpstream();
    }, 30);

    const inFlightRes = await inFlightPromise;
    assert.equal(inFlightRes.statusCode, 200);
    assert.equal(inFlightRes.body.toString('utf8'), 'finished-before-shutdown');
    assert.equal(inFlightRes.headers.connection, 'close');

    await closePromise;
    assert.equal(proxy.getActiveRequests(), 0);
  });
});
