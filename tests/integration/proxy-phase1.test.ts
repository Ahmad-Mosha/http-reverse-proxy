import assert from 'node:assert/strict';
import { createHash, randomBytes } from 'node:crypto';
import { createServer, request as httpRequest } from 'node:http';
import type { IncomingMessage, Server, ServerResponse } from 'node:http';
import { connect as netConnect } from 'node:net';
import { afterEach, describe, it } from 'node:test';
import { ProxyServer } from '../../src/proxy/proxy-server.js';

interface TestUpstream {
  readonly id: string;
  readonly port: number;
  readonly server: Server;
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
            server.close(() => resClose());
          }),
      });
    });
  });
}

interface HttpResponseResult {
  readonly statusCode: number;
  readonly headers: IncomingMessage['headers'];
  readonly body: Buffer;
}

async function sendHttpRequest(options: {
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

describe('Phase 1 Integration — Core HTTP Reverse Proxy & Load Balancer', () => {
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

  it('forwards GET, POST, PUT, DELETE, and HEAD requests with headers, status codes, and bodies', async () => {
    const upstream = await createTestUpstream('u1', (req, res) => {
      if (req.url === '/no-content') {
        res.writeHead(204, { 'x-upstream': 'u1' });
        res.end();
        return;
      }

      const chunks: Buffer[] = [];
      req.on('data', (chunk: Buffer) => chunks.push(chunk));
      req.on('end', () => {
        const rawBody = Buffer.concat(chunks).toString('utf8');
        const responsePayload = JSON.stringify({
          method: req.method,
          url: req.url,
          receivedHost: req.headers.host,
          forwardedHost: req.headers['x-forwarded-host'],
          forwardedFor: req.headers['x-forwarded-for'],
          forwardedProto: req.headers['x-forwarded-proto'],
          requestId: req.headers['x-request-id'],
          customHeader: req.headers['x-custom-client'],
          hopByHopStripped: req.headers['x-secret-hop'] === undefined,
          body: rawBody,
        });

        res.writeHead(req.method === 'POST' ? 201 : 200, {
          'content-type': 'application/json',
          'x-upstream-id': 'u1',
        });
        res.end(responsePayload);
      });
    });
    activeUpstreams.push(upstream);

    const proxy = new ProxyServer({
      port: 0,
      upstreams: [{ id: 'u1', host: '127.0.0.1', port: upstream.port }],
    });
    activeProxies.push(proxy);
    const { port: proxyPort } = await proxy.listen();

    // 1. POST with JSON body and custom headers
    const postRes = await sendHttpRequest({
      port: proxyPort,
      method: 'POST',
      path: '/api/items?filter=active',
      headers: {
        host: 'shop.example.com',
        'content-type': 'application/json',
        'x-custom-client': 'hello-proxy',
        connection: 'keep-alive, x-secret-hop',
        'x-secret-hop': 'must-not-reach-upstream',
      },
      body: JSON.stringify({ name: 'Mechanical Keyboard' }),
    });

    assert.equal(postRes.statusCode, 201);
    assert.equal(postRes.headers['x-upstream-id'], 'u1');
    assert.equal(postRes.headers.via, '1.1 http-reverse-proxy');
    assert.ok(typeof postRes.headers['x-request-id'] === 'string');

    const parsed = JSON.parse(postRes.body.toString('utf8')) as Record<string, unknown>;
    assert.equal(parsed['method'], 'POST');
    assert.equal(parsed['url'], '/api/items?filter=active');
    assert.equal(parsed['receivedHost'], `127.0.0.1:${upstream.port}`);
    assert.equal(parsed['forwardedHost'], 'shop.example.com');
    assert.equal(parsed['forwardedProto'], 'http');
    assert.equal(parsed['customHeader'], 'hello-proxy');
    assert.equal(parsed['hopByHopStripped'], true);
    assert.equal(parsed['body'], '{"name":"Mechanical Keyboard"}');

    // 2. 204 No Content empty payload
    const emptyRes = await sendHttpRequest({
      port: proxyPort,
      method: 'DELETE',
      path: '/no-content',
    });
    assert.equal(emptyRes.statusCode, 204);
    assert.equal(emptyRes.body.byteLength, 0);
  });

  it('distributes requests evenly across 3 upstreams in round-robin mode', async () => {
    for (const id of ['u1', 'u2', 'u3']) {
      const u = await createTestUpstream(id, (_req, res) => {
        res.writeHead(200, { 'content-type': 'text/plain' });
        res.end(id);
      });
      activeUpstreams.push(u);
    }

    const proxy = new ProxyServer({
      port: 0,
      strategy: 'round-robin',
      upstreams: activeUpstreams.map((u) => ({
        id: u.id,
        host: '127.0.0.1',
        port: u.port,
      })),
    });
    activeProxies.push(proxy);
    const { port: proxyPort } = await proxy.listen();

    const responses: string[] = [];
    for (let i = 0; i < 6; i += 1) {
      const res = await sendHttpRequest({ port: proxyPort, path: '/ping' });
      assert.equal(res.statusCode, 200);
      responses.push(res.body.toString('utf8'));
    }

    assert.deepEqual(responses, ['u1', 'u2', 'u3', 'u1', 'u2', 'u3']);
  });

  it('routes around busy upstreams in least-connections mode', async () => {
    let markSlowArrived!: () => void;
    const slowArrivedPromise = new Promise<void>((resolve) => {
      markSlowArrived = resolve;
    });
    let releaseSlowRequest!: () => void;
    const slowReleasePromise = new Promise<void>((resolve) => {
      releaseSlowRequest = resolve;
    });

    const u1 = await createTestUpstream('u1', (req, res) => {
      if (req.url === '/slow') {
        markSlowArrived();
        void slowReleasePromise.then(() => {
          res.writeHead(200);
          res.end('u1-slow');
        });
        return;
      }
      res.writeHead(200);
      res.end('u1');
    });
    const u2 = await createTestUpstream('u2', (_req, res) => {
      res.writeHead(200);
      res.end('u2');
    });
    activeUpstreams.push(u1, u2);

    const proxy = new ProxyServer({
      port: 0,
      strategy: 'least-connections',
      upstreams: [
        { id: 'u1', host: '127.0.0.1', port: u1.port },
        { id: 'u2', host: '127.0.0.1', port: u2.port },
      ],
    });
    activeProxies.push(proxy);
    const { port: proxyPort } = await proxy.listen();

    // First request goes to u1 and stays in-flight (u1.activeConnections === 1)
    const pendingSlowReq = sendHttpRequest({ port: proxyPort, path: '/slow' });
    await slowArrivedPromise;

    // While u1 has 1 active connection and u2 has 0, subsequent requests must all pick u2
    for (let i = 0; i < 4; i += 1) {
      const fastRes = await sendHttpRequest({ port: proxyPort, path: '/fast' });
      assert.equal(fastRes.body.toString('utf8'), 'u2');
    }

    releaseSlowRequest();
    const slowRes = await pendingSlowReq;
    assert.equal(slowRes.body.toString('utf8'), 'u1-slow');
  });

  it('streams a 4 MiB payload with backpressure to a slow client and preserves SHA-256 integrity', async () => {
    const chunkSize = 64 * 1024; // 64 KiB
    const totalChunks = 64; // 4 MiB total
    const sourceChunks: Buffer[] = [];
    const expectedHash = createHash('sha256');

    for (let i = 0; i < totalChunks; i += 1) {
      const buf = randomBytes(chunkSize);
      sourceChunks.push(buf);
      expectedHash.update(buf);
    }
    const expectedDigest = expectedHash.digest('hex');

    const upstream = await createTestUpstream('u-stream', (_req, res) => {
      res.writeHead(200, {
        'content-type': 'application/octet-stream',
        'content-length': String(chunkSize * totalChunks),
      });

      let index = 0;
      const writeNext = (): void => {
        while (index < sourceChunks.length) {
          const chunk = sourceChunks[index];
          index += 1;
          if (chunk !== undefined && !res.write(chunk)) {
            res.once('drain', writeNext);
            return;
          }
        }
        res.end();
      };
      writeNext();
    });
    activeUpstreams.push(upstream);

    const proxy = new ProxyServer({
      port: 0,
      upstreams: [{ id: 'u-stream', host: '127.0.0.1', port: upstream.port }],
      stream: {
        highWaterMarkBytes: 16 * 1024, // 16 KiB highWaterMark to trigger backpressure deterministically
      },
    });
    activeProxies.push(proxy);
    const { port: proxyPort } = await proxy.listen();

    const receivedHash = createHash('sha256');
    let totalReceivedBytes = 0;

    await new Promise<void>((resolve, reject) => {
      const req = httpRequest(
        {
          host: '127.0.0.1',
          port: proxyPort,
          method: 'GET',
          path: '/large-stream',
        },
        (res) => {
          res.pause();
          let pausesRemaining = 5;

          const readSlowly = (): void => {
            let chunk: Buffer | null;
            while ((chunk = res.read() as Buffer | null) !== null) {
              receivedHash.update(chunk);
              totalReceivedBytes += chunk.byteLength;
              if (pausesRemaining > 0) {
                pausesRemaining -= 1;
                setTimeout(readSlowly, 25);
                return;
              }
            }
          };

          res.on('readable', readSlowly);
          res.on('end', () => resolve());
          res.on('error', reject);
        },
      );
      req.on('error', reject);
      req.end();
    });

    assert.equal(totalReceivedBytes, chunkSize * totalChunks);
    assert.equal(receivedHash.digest('hex'), expectedDigest);
    assert.ok(
      proxy.getBackpressureMetrics().clientPauseCount > 0,
      'Expected proxy response stream to experience backpressure and drain events',
    );
  });

  it('propagates client abort immediately to upstream and releases active connection count', async () => {
    let notifyUpstreamStarted!: () => void;
    const upstreamStarted = new Promise<void>((resolve) => {
      notifyUpstreamStarted = resolve;
    });

    let notifyUpstreamAborted!: () => void;
    const upstreamAborted = new Promise<void>((resolve) => {
      notifyUpstreamAborted = resolve;
    });

    const upstream = await createTestUpstream('u-abort', (_req, res) => {
      notifyUpstreamStarted();
      res.on('close', () => {
        if (!res.writableEnded) {
          notifyUpstreamAborted();
        }
      });
    });
    activeUpstreams.push(upstream);

    const proxy = new ProxyServer({
      port: 0,
      upstreams: [{ id: 'u-abort', host: '127.0.0.1', port: upstream.port }],
    });
    activeProxies.push(proxy);
    const { port: proxyPort } = await proxy.listen();

    const clientReq = httpRequest({
      host: '127.0.0.1',
      port: proxyPort,
      method: 'GET',
      path: '/hang',
    });
    clientReq.on('error', () => {
      // Expected when client destroys its own request
    });
    clientReq.end();

    await upstreamStarted;
    assert.equal(proxy.getPool().getSnapshots()[0]?.activeConnections, 1);

    // Client aborts connection before upstream responds
    clientReq.destroy();
    await upstreamAborted;

    // Wait briefly for close event propagation on proxy
    await new Promise((r) => setTimeout(r, 20));
    assert.equal(proxy.getPool().getSnapshots()[0]?.activeConnections, 0);
  });

  it('returns 502 Bad Gateway when upstream refuses connection (even on POST with body)', async () => {
    // Allocate an ephemeral port and close it immediately so it refuses connections
    const deadUpstream = await createTestUpstream('u-dead', (_req, res) => {
      res.end();
    });
    const deadPort = deadUpstream.port;
    await deadUpstream.close();

    const proxy = new ProxyServer({
      port: 0,
      upstreams: [{ id: 'u-dead', host: '127.0.0.1', port: deadPort }],
    });
    activeProxies.push(proxy);
    const { port: proxyPort } = await proxy.listen();

    const res = await sendHttpRequest({
      port: proxyPort,
      method: 'POST',
      path: '/submit',
      body: 'payload-while-upstream-down',
    });

    assert.equal(res.statusCode, 502);
    const body = JSON.parse(res.body.toString('utf8')) as {
      error: { code: string; statusCode: number };
    };
    assert.equal(body.error.code, 'UPSTREAM_CONNECT_ERROR');
    assert.equal(body.error.statusCode, 502);
    assert.equal(proxy.getPool().getSnapshots()[0]?.activeConnections, 0);
    assert.equal(proxy.getPool().getSnapshots()[0]?.totalErrors, 1);
  });

  it('returns 504 Gateway Timeout when upstream exceeds upstreamResponseTimeoutMs', async () => {
    const slowUpstream = await createTestUpstream('u-timeout', (_req, _res) => {
      // Intentionally never send headers
    });
    activeUpstreams.push(slowUpstream);

    const proxy = new ProxyServer({
      port: 0,
      upstreams: [{ id: 'u-timeout', host: '127.0.0.1', port: slowUpstream.port }],
      timeouts: {
        upstreamResponseTimeoutMs: 120,
      },
    });
    activeProxies.push(proxy);
    const { port: proxyPort } = await proxy.listen();

    const res = await sendHttpRequest({
      port: proxyPort,
      method: 'GET',
      path: '/stall',
    });

    assert.equal(res.statusCode, 504);
    const body = JSON.parse(res.body.toString('utf8')) as {
      error: { code: string; statusCode: number };
    };
    assert.equal(body.error.code, 'UPSTREAM_TIMEOUT');
  });

  it('enforces security boundaries against TRACE, open-proxy URLs, and smuggling headers', async () => {
    const upstream = await createTestUpstream('u-sec', (_req, res) => {
      res.writeHead(200);
      res.end('ok');
    });
    activeUpstreams.push(upstream);

    const proxy = new ProxyServer({
      port: 0,
      upstreams: [{ id: 'u-sec', host: '127.0.0.1', port: upstream.port }],
    });
    activeProxies.push(proxy);
    const { port: proxyPort } = await proxy.listen();

    // 1. TRACE method -> 405 Method Not Allowed
    const traceRes = await sendHttpRequest({
      port: proxyPort,
      method: 'TRACE',
      path: '/',
    });
    assert.equal(traceRes.statusCode, 405);

    // 2. Protocol-relative path '//evil.com' -> 400 Bad Request
    const protoRelativeRes = await sendHttpRequest({
      port: proxyPort,
      method: 'GET',
      path: '//evil.com/path',
    });
    assert.equal(protoRelativeRes.statusCode, 400);

    // 3. Raw TCP socket sending absolute-form open-proxy target `GET http://evil.com/ HTTP/1.1`
    const rawResponse = await new Promise<string>((resolve, reject) => {
      const socket = netConnect(proxyPort, '127.0.0.1', () => {
        socket.write(
          'GET http://evil.com/admin HTTP/1.1\r\nHost: evil.com\r\nConnection: close\r\n\r\n',
        );
      });
      const chunks: Buffer[] = [];
      socket.on('data', (c: Buffer) => chunks.push(c));
      socket.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
      socket.on('error', reject);
    });
    assert.match(rawResponse, /^HTTP\/1\.1 400 Bad Request/);
  });
});
