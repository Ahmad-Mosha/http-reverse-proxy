import { randomUUID } from 'node:crypto';
import { createServer } from 'node:http';
import type { IncomingMessage, Server, ServerResponse } from 'node:http';
import type { Duplex, Writable } from 'node:stream';
import { performance } from 'node:perf_hooks';
import { createProxyConfig } from '../config/validator.js';
import { ProxyError, sendProxyError } from '../http/errors.js';
import { StructuredLogger } from '../observability/logger.js';
import { MetricsCollector } from '../observability/metrics.js';
import type { ProxyMetricsSnapshot } from '../observability/metrics.js';
import { HealthChecker } from '../pool/health-checker.js';
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
  private readonly healthChecker: HealthChecker;
  private readonly metrics: MetricsCollector;
  private readonly logger: StructuredLogger;
  private readonly server: Server;
  private readonly backpressureMetrics: BackpressureMetrics = {
    clientPauseCount: 0,
    upstreamPauseCount: 0,
  };
  private boundPort: number | null = null;
  private activeRequests = 0;
  private draining = false;
  private closingPromise: Promise<void> | null = null;

  constructor(inputConfig: PartialProxyConfig, logStream?: Writable) {
    this.config = createProxyConfig(inputConfig);
    this.pool = new UpstreamPool(
      this.config.upstreams,
      this.config.pool,
      this.config.timeouts,
      this.config.healthCheck,
    );
    this.loadBalancer = new LoadBalancer(
      this.pool,
      this.config.strategy,
      this.config.limits.maxActiveRequestsPerUpstream,
    );
    this.healthChecker = new HealthChecker(this.pool, this.config.healthCheck);
    this.metrics = new MetricsCollector();
    this.logger = new StructuredLogger(
      logStream !== undefined
        ? { level: this.config.observability.logLevel, stream: logStream }
        : { level: this.config.observability.logLevel },
    );

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
    const urlPath = (req.url ?? '/').split('?')[0] ?? '/';
    const method = (req.method ?? 'GET').toUpperCase();

    if (method === 'GET' && urlPath === this.config.observability.metricsPath) {
      req.resume();
      const body = JSON.stringify(this.getMetricsSnapshot());
      res.writeHead(200, {
        'content-type': 'application/json; charset=utf-8',
        'cache-control': 'no-store',
      });
      res.end(body);
      return;
    }

    if (method === 'GET' && urlPath === this.config.observability.healthPath) {
      req.resume();
      const healthyCount = this.pool.getHealthyUpstreams().length;
      const statusCode = !this.draining && healthyCount > 0 ? 200 : 503;
      const body = JSON.stringify({
        status: statusCode === 200 ? 'healthy' : 'unhealthy',
        draining: this.draining,
        healthyUpstreams: healthyCount,
        totalUpstreams: this.pool.getAllUpstreams().length,
      });
      res.writeHead(statusCode, {
        'content-type': 'application/json; charset=utf-8',
        'cache-control': 'no-store',
      });
      res.end(body);
      return;
    }

    const incomingReqId = req.headers['x-request-id'];
    const requestId =
      typeof incomingReqId === 'string' && incomingReqId.trim().length > 0
        ? incomingReqId.trim()
        : randomUUID();

    if (this.draining) {
      req.resume();
      res.shouldKeepAlive = false;
      const err = new ProxyError(
        503,
        'SERVER_SHUTTING_DOWN',
        'Proxy server is shutting down and not accepting new requests',
      );
      this.metrics.recordRequestComplete({
        statusCode: 503,
        durationMs: 0,
        retries: 0,
        errorCode: err.code,
      });
      sendProxyError(res, err, requestId, true);
      return;
    }

    if (this.activeRequests >= this.config.limits.maxActiveRequests) {
      req.resume();
      const err = new ProxyError(
        503,
        'OVERLOADED',
        `Maximum concurrent proxy requests (${this.config.limits.maxActiveRequests}) reached`,
      );
      this.metrics.recordRequestComplete({
        statusCode: 503,
        durationMs: 0,
        retries: 0,
        errorCode: err.code,
      });
      sendProxyError(res, err, requestId);
      return;
    }

    this.activeRequests += 1;
    const ctx: RequestContext = {
      requestId,
      method,
      url: req.url ?? '/',
      clientIp: req.socket.remoteAddress ?? '127.0.0.1',
      startTimeMs: performance.now(),
      attempts: 0,
    };

    let finalized = false;
    const onComplete = (): void => {
      if (finalized) {
        return;
      }
      finalized = true;
      this.activeRequests -= 1;

      const durationMs = Math.round((performance.now() - ctx.startTimeMs) * 100) / 100;
      const statusCode = ctx.statusCode ?? res.statusCode;
      const retries = ctx.attempts > 1 ? ctx.attempts - 1 : 0;

      const metricRecord: {
        statusCode: number;
        durationMs: number;
        upstreamLatencyMs?: number;
        retries: number;
        errorCode?: ProxyError['code'];
      } = {
        statusCode,
        durationMs,
        retries,
      };
      if (ctx.upstreamLatencyMs !== undefined) {
        metricRecord.upstreamLatencyMs = ctx.upstreamLatencyMs;
      }
      if (ctx.errorCode !== undefined) {
        metricRecord.errorCode = ctx.errorCode;
      }
      this.metrics.recordRequestComplete(metricRecord);

      if (statusCode >= 500) {
        this.logger.error('request.completed', {
          requestId: ctx.requestId,
          method: ctx.method,
          url: ctx.url,
          statusCode,
          upstreamId: ctx.upstreamId,
          durationMs,
          upstreamLatencyMs: ctx.upstreamLatencyMs,
          retries,
          errorCode: ctx.errorCode,
        });
      } else {
        this.logger.info('request.completed', {
          requestId: ctx.requestId,
          method: ctx.method,
          url: ctx.url,
          statusCode,
          upstreamId: ctx.upstreamId,
          durationMs,
          upstreamLatencyMs: ctx.upstreamLatencyMs,
          retries,
        });
      }
    };
    res.on('close', onComplete);

    forwardRequest(
      req,
      res,
      ctx,
      {
        config: this.config,
        pool: this.pool,
        loadBalancer: this.loadBalancer,
        isDraining: () => this.draining,
      },
      this.backpressureMetrics,
    );
  }

  async listen(): Promise<{ host: string; port: number }> {
    return new Promise((resolve, reject) => {
      const onError = (err: Error): void => {
        this.server.removeListener('listening', onListening);
        reject(err);
      };

      const onListening = (): void => {
        this.server.removeListener('error', onError);
        this.draining = false;
        this.healthChecker.start();
        const addr = this.server.address();
        const bound =
          addr !== null && typeof addr === 'object'
            ? { host: addr.address, port: addr.port }
            : { host: this.config.host, port: this.config.port };
        this.boundPort = bound.port;
        this.logger.info('proxy.listening', bound);
        resolve(bound);
      };

      this.server.once('error', onError);
      this.server.once('listening', onListening);
      this.server.listen(this.config.port, this.config.host);
    });
  }

  async close(): Promise<void> {
    if (this.closingPromise !== null) {
      return this.closingPromise;
    }

    this.draining = true;
    this.healthChecker.stop();

    if (!this.server.listening) {
      this.pool.destroy();
      this.metrics.stop();
      this.boundPort = null;
      return;
    }

    this.closingPromise = new Promise<void>((resolve, reject) => {
      const forceTimer = setTimeout(() => {
        this.server.closeAllConnections();
        this.pool.destroy();
      }, this.config.timeouts.shutdownGracePeriodMs);
      forceTimer.unref();

      this.server.close((err) => {
        clearTimeout(forceTimer);
        this.pool.destroy();
        this.metrics.stop();
        this.boundPort = null;
        this.closingPromise = null;
        if (err !== undefined) {
          reject(err);
          return;
        }
        resolve();
      });

      this.server.closeIdleConnections();
    });

    return this.closingPromise;
  }

  async runHealthCheckCycle(): Promise<void> {
    await this.healthChecker.runProbeCycle();
  }

  getMetricsSnapshot(): ProxyMetricsSnapshot {
    return this.metrics.snapshot({
      activeRequests: this.activeRequests,
      clientPauseCount: this.backpressureMetrics.clientPauseCount,
      upstreamPauseCount: this.backpressureMetrics.upstreamPauseCount,
      upstreams: this.pool.getSnapshots(),
    });
  }

  resetEventLoopStats(): void {
    this.metrics.resetEventLoopStats();
  }

  getConfig(): ProxyConfig {
    return this.config;
  }

  getPool(): UpstreamPool {
    return this.pool;
  }

  getActiveRequests(): number {
    return this.activeRequests;
  }

  isDraining(): boolean {
    return this.draining;
  }

  getBackpressureMetrics(): Readonly<BackpressureMetrics> {
    return this.backpressureMetrics;
  }

  getPort(): number | null {
    return this.boundPort;
  }
}
