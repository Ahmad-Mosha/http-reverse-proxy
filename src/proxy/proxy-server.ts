import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import type { IncomingMessage, Server, ServerResponse } from 'node:http';
import type { Duplex } from 'node:stream';
import { performance } from 'node:perf_hooks';
import { createProxyConfig } from '../config/validator.js';
import { LoadBalancer } from '../pool/load-balancer.js';
import { UpstreamPool } from '../pool/upstream-pool.js';
import type { PartialProxyConfig, ProxyConfig } from '../types/config.js';
import type { RequestContext } from '../types/context.js';
import { forwardRequest } from './forwarder.js';
import type { BackpressureMetrics } from './forwarder.js';

export class ProxyServer {
  private readonly config: ProxyConfig;
  private readonly pool: UpstreamPool;
  private readonly loadBalancer: LoadBalancer;
  private readonly server: Server;
  private readonly backpressureMetrics: BackpressureMetrics = {
    clientPauseCount: 0,
    upstreamPauseCount: 0,
  };
  private boundPort: number | null = null;

  public constructor(inputConfig: PartialProxyConfig) {
    this.config = createProxyConfig(inputConfig);
    this.pool = new UpstreamPool(
      this.config.upstreams,
      this.config.pool,
      this.config.timeouts,
    );
    this.loadBalancer = new LoadBalancer(this.pool, this.config.strategy);

    this.server = createServer(
      {
        highWaterMark: this.config.stream.highWaterMarkBytes,
      },
      (req: IncomingMessage, res: ServerResponse) => {
        this.handleRequest(req, res);
      },
    );

    this.server.keepAliveTimeout = this.config.timeouts.clientKeepAliveTimeoutMs;
    this.server.headersTimeout = this.config.timeouts.clientHeadersTimeoutMs;
    this.server.requestTimeout = this.config.timeouts.clientRequestTimeoutMs;

    this.server.on('connection', (socket) => {
      socket.setNoDelay(true);
    });

    this.server.on('clientError', (_err: Error, socket: Duplex) => {
      if (socket.writable && !socket.destroyed) {
        socket.end(
          'HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n',
        );
      }
      socket.destroy();
    });
  }

  private handleRequest(req: IncomingMessage, res: ServerResponse): void {
    const incomingReqId = req.headers['x-request-id'];
    const requestId =
      typeof incomingReqId === 'string' && incomingReqId.trim().length > 0
        ? incomingReqId.trim()
        : randomUUID();

    const ctx: RequestContext = {
      requestId,
      method: req.method ?? 'GET',
      url: req.url ?? '/',
      clientIp: req.socket.remoteAddress ?? '127.0.0.1',
      startTimeMs: performance.now(),
    };

    forwardRequest(
      req,
      res,
      ctx,
      {
        config: this.config,
        pool: this.pool,
        loadBalancer: this.loadBalancer,
      },
      this.backpressureMetrics,
    );
  }

  public async listen(): Promise<{ host: string; port: number }> {
    return new Promise((resolve, reject) => {
      const onError = (err: Error): void => {
        this.server.removeListener('listening', onListening);
        reject(err);
      };

      const onListening = (): void => {
        this.server.removeListener('error', onError);
        const addr = this.server.address();
        if (addr !== null && typeof addr === 'object') {
          this.boundPort = addr.port;
          resolve({ host: addr.address, port: addr.port });
        } else {
          this.boundPort = this.config.port;
          resolve({ host: this.config.host, port: this.config.port });
        }
      };

      this.server.once('error', onError);
      this.server.once('listening', onListening);
      this.server.listen(this.config.port, this.config.host);
    });
  }

  public async close(): Promise<void> {
    this.pool.destroy();
    if (!this.server.listening) {
      this.boundPort = null;
      return;
    }

    return new Promise((resolve, reject) => {
      this.server.close((err) => {
        this.boundPort = null;
        if (err !== undefined) {
          reject(err);
          return;
        }
        resolve();
      });
    });
  }

  public getConfig(): ProxyConfig {
    return this.config;
  }

  public getPool(): UpstreamPool {
    return this.pool;
  }

  public getBackpressureMetrics(): Readonly<BackpressureMetrics> {
    return this.backpressureMetrics;
  }

  public getPort(): number | null {
    return this.boundPort;
  }
}
