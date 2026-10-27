import { createServer } from 'node:http';
import type { Server } from 'node:http';
import { ProxyServer } from '../proxy/proxy-server.js';

interface DemoUpstream {
  readonly id: string;
  readonly port: number;
  readonly server: Server;
}

async function startDemoUpstream(id: string, port: number): Promise<DemoUpstream> {
  const server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', (chunk: Buffer) => {
      chunks.push(chunk);
    });
    req.on('end', () => {
      const body = Buffer.concat(chunks).toString('utf8');
      const payload = JSON.stringify({
        upstreamId: id,
        method: req.method,
        url: req.url,
        headers: req.headers,
        bodyLength: body.length,
        timestamp: new Date().toISOString(),
      });
      res.writeHead(200, {
        'content-type': 'application/json; charset=utf-8',
        'x-upstream-id': id,
      });
      res.end(payload);
    });
  });

  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', () => {
      const addr = server.address();
      const boundPort = typeof addr === 'object' && addr !== null ? addr.port : port;
      resolve({ id, port: boundPort, server });
    });
  });
}

async function main(): Promise<void> {
  const upstreams = await Promise.all([
    startDemoUpstream('upstream-1', 0),
    startDemoUpstream('upstream-2', 0),
    startDemoUpstream('upstream-3', 0),
  ]);

  const proxyPort = Number(process.env['PORT'] ?? '8080');
  const proxy = new ProxyServer({
    host: '127.0.0.1',
    port: proxyPort,
    strategy: 'round-robin',
    upstreams: upstreams.map((u) => ({
      id: u.id,
      host: '127.0.0.1',
      port: u.port,
    })),
  });

  const { host, port } = await proxy.listen();
  process.stdout.write(
    `RelayEdge listening on http://${host}:${port} -> [${upstreams.map((u) => `${u.id}@127.0.0.1:${u.port}`).join(', ')}]\n`,
  );

  const shutdown = async (): Promise<void> => {
    process.stdout.write('\nShutting down RelayEdge and demo upstreams...\n');
    await proxy.close();
    await Promise.all(
      upstreams.map(
        (u) =>
          new Promise<void>((resolve) => {
            u.server.close(() => resolve());
          }),
      ),
    );
    process.exit(0);
  };

  process.once('SIGINT', () => void shutdown());
  process.once('SIGTERM', () => void shutdown());
}

void main();
