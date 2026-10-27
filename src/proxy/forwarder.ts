import { request as httpRequest } from 'node:http';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { pipeline } from 'node:stream';
import {
  classifyUpstreamError,
  ProxyError,
  sendProxyError,
  validateIncomingRequest,
} from '../http/errors.js';
import {
  buildClientResponseHeaders,
  buildUpstreamRequestHeaders,
} from '../http/headers.js';
import type { LoadBalancer } from '../pool/load-balancer.js';
import type { UpstreamPool } from '../pool/upstream-pool.js';
import type { ProxyConfig } from '../types/config.js';
import type { RequestContext } from '../types/context.js';

export interface ForwarderDependencies {
  readonly config: ProxyConfig;
  readonly pool: UpstreamPool;
  readonly loadBalancer: LoadBalancer;
}

export interface BackpressureMetrics {
  clientPauseCount: number;
  upstreamPauseCount: number;
}

/**
 * Forwards an incoming client HTTP/1.1 request to a selected healthy upstream server
 * using non-buffered Node.js streams with end-to-end backpressure propagation.
 */
export function forwardRequest(
  req: IncomingMessage,
  res: ServerResponse,
  ctx: RequestContext,
  deps: ForwarderDependencies,
  backpressureMetrics?: BackpressureMetrics,
): void {
  const { config, pool, loadBalancer } = deps;

  try {
    validateIncomingRequest(req);
  } catch (err) {
    const proxyErr =
      err instanceof ProxyError
        ? err
        : new ProxyError(400, 'BAD_REQUEST', 'Invalid HTTP request');
    ctx.statusCode = proxyErr.statusCode;
    req.resume();
    sendProxyError(res, proxyErr, ctx.requestId);
    return;
  }

  let upstream;
  try {
    upstream = loadBalancer.selectUpstream();
  } catch (err) {
    const proxyErr =
      err instanceof ProxyError
        ? err
        : new ProxyError(503, 'NO_HEALTHY_UPSTREAM', 'No healthy upstreams');
    ctx.statusCode = proxyErr.statusCode;
    req.resume();
    sendProxyError(res, proxyErr, ctx.requestId);
    return;
  }

  ctx.upstreamId = upstream.id;
  pool.acquireConnection(upstream);

  let connectionReleased = false;
  const releaseConnectionOnce = (): void => {
    if (!connectionReleased) {
      connectionReleased = true;
      pool.releaseConnection(upstream);
    }
  };

  let responseReceived = false;
  let clientAborted = false;
  let errorHandled = false;

  const responseTimeoutTimer = setTimeout(() => {
    if (!responseReceived) {
      const timeoutError = new ProxyError(
        504,
        'UPSTREAM_TIMEOUT',
        `Upstream "${upstream.id}" did not send response headers within ${config.timeouts.upstreamResponseTimeoutMs}ms`,
      );
      upstreamReq.destroy(timeoutError);
    }
  }, config.timeouts.upstreamResponseTimeoutMs);
  responseTimeoutTimer.unref();

  const clearResponseTimeout = (): void => {
    clearTimeout(responseTimeoutTimer);
  };

  const handleUpstreamFailure = (err: unknown): void => {
    if (errorHandled) {
      return;
    }
    errorHandled = true;
    clearResponseTimeout();
    releaseConnectionOnce();

    req.unpipe(upstreamReq);
    req.resume();

    if (clientAborted) {
      return;
    }

    pool.recordError(upstream);
    const proxyErr = classifyUpstreamError(err, upstream.id);
    ctx.statusCode = proxyErr.statusCode;
    sendProxyError(res, proxyErr, ctx.requestId);
  };

  const upstreamHeaders = buildUpstreamRequestHeaders({
    incomingHeaders: req.headers,
    upstream,
    clientIp: ctx.clientIp,
    requestId: ctx.requestId,
    trustProxy: config.trustProxy,
  });

  const upstreamReq = httpRequest({
    protocol: upstream.protocol,
    host: upstream.host,
    port: upstream.port,
    method: req.method ?? 'GET',
    path: req.url ?? '/',
    headers: upstreamHeaders,
    agent: upstream.agent,
  });

  // Disable Nagle's algorithm on outbound socket for low-latency header & chunk flushing
  upstreamReq.on('socket', (socket) => {
    socket.setNoDelay(true);
  });

  // Track if the client disconnects prematurely before the response finishes
  res.on('close', () => {
    clearResponseTimeout();
    releaseConnectionOnce();
    if (!res.writableEnded) {
      clientAborted = true;
      req.unpipe(upstreamReq);
      upstreamReq.destroy();
    }
  });

  req.on('error', (err) => {
    if (!responseReceived) {
      clientAborted = true;
      upstreamReq.destroy(err);
    }
  });

  // Observe backpressure on outbound request stream (Fast Client -> Slow Upstream)
  if (backpressureMetrics !== undefined) {
    upstreamReq.on('drain', () => {
      backpressureMetrics.upstreamPauseCount += 1;
    });
  }

  upstreamReq.on('error', (err) => {
    handleUpstreamFailure(err);
  });

  upstreamReq.on('response', (upstreamRes: IncomingMessage) => {
    responseReceived = true;
    clearResponseTimeout();

    if (clientAborted || res.destroyed) {
      upstreamRes.resume();
      releaseConnectionOnce();
      return;
    }

    const statusCode = upstreamRes.statusCode ?? 502;
    ctx.statusCode = statusCode;

    const responseHeaders = buildClientResponseHeaders(
      upstreamRes.headers,
      ctx.requestId,
    );

    if (upstreamRes.statusMessage !== undefined && upstreamRes.statusMessage.length > 0) {
      res.writeHead(statusCode, upstreamRes.statusMessage, responseHeaders);
    } else {
      res.writeHead(statusCode, responseHeaders);
    }

    // Observe backpressure on downstream response stream (Fast Upstream -> Slow Client)
    if (backpressureMetrics !== undefined) {
      res.on('drain', () => {
        backpressureMetrics.clientPauseCount += 1;
      });
    }

    // Stream response body from upstream to client with automatic backpressure & error propagation
    pipeline(upstreamRes, res, (err) => {
      releaseConnectionOnce();
      if (err !== null && !clientAborted) {
        handleUpstreamFailure(err);
      }
    });
  });

  // Stream request body from client to upstream using .pipe() so that if upstreamReq fails
  // before req completes, we can unpipe and send a valid HTTP 502 response without
  // pipeline() calling req.destroy() and resetting the client TCP socket prematurely.
  req.pipe(upstreamReq);
}
