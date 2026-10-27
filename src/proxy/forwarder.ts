import { request as httpRequest } from 'node:http';
import type { IncomingMessage, ServerResponse } from 'node:http';
import { pipeline } from 'node:stream';
import {
  classifyUpstreamError,
  isRetryableMethod,
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
  config: ProxyConfig;
  pool: UpstreamPool;
  loadBalancer: LoadBalancer;
  isDraining?: () => boolean;
}

export interface BackpressureMetrics {
  clientPauseCount: number;
  upstreamPauseCount: number;
}

export function forwardRequest(
  req: IncomingMessage,
  res: ServerResponse,
  ctx: RequestContext,
  deps: ForwarderDependencies,
  backpressureMetrics?: BackpressureMetrics,
): void {
  const { config, pool, loadBalancer, isDraining } = deps;

  try {
    validateIncomingRequest(req, config.limits.maxRequestBodyBytes);
  } catch (err) {
    const proxyErr =
      err instanceof ProxyError
        ? err
        : new ProxyError(400, 'BAD_REQUEST', 'Invalid HTTP request');
    ctx.statusCode = proxyErr.statusCode;
    req.resume();
    sendProxyError(res, proxyErr, ctx.requestId, proxyErr.statusCode === 413);
    return;
  }

  const canRetry = isRetryableMethod(req.method) && config.limits.maxRetries > 0;
  const attemptedUpstreams = new Set<string>();

  const executeAttempt = (): void => {
    let upstream;
    try {
      upstream = loadBalancer.selectUpstream(
        attemptedUpstreams.size > 0 ? attemptedUpstreams : undefined,
      );
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

    ctx.attempts += 1;
    ctx.upstreamId = upstream.id;
    attemptedUpstreams.add(upstream.id);
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
    let attemptSettled = false;

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

    const cleanupListeners = (): void => {
      res.removeListener('close', onClientClose);
      req.removeListener('error', onClientReqError);
      req.removeListener('data', onRequestBodyChunk);
    };

    const handleAttemptFailure = (err: unknown): void => {
      if (attemptSettled) {
        return;
      }
      attemptSettled = true;
      clearResponseTimeout();
      releaseConnectionOnce();
      cleanupListeners();

      req.unpipe(upstreamReq);

      if (clientAborted) {
        return;
      }

      if (err instanceof ProxyError && err.code === 'PAYLOAD_TOO_LARGE') {
        req.resume();
        ctx.statusCode = err.statusCode;
        sendProxyError(res, err, ctx.requestId, true);
        return;
      }

      pool.recordError(upstream);

      // Safe retry: only for idempotent bodyless methods (GET/HEAD/OPTIONS) when failure
      // happened before any response headers were received from the upstream.
      if (
        canRetry &&
        !responseReceived &&
        ctx.attempts <= config.limits.maxRetries &&
        pool.getHealthyUpstreams().some((u) => !attemptedUpstreams.has(u.id))
      ) {
        executeAttempt();
        return;
      }

      req.resume();
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

    upstreamReq.on('socket', (socket) => {
      socket.setNoDelay(true);
    });

    const onClientClose = (): void => {
      clearResponseTimeout();
      releaseConnectionOnce();
      cleanupListeners();
      if (!res.writableEnded) {
        clientAborted = true;
        req.unpipe(upstreamReq);
        upstreamReq.destroy();
      }
    };

    const onClientReqError = (err: Error): void => {
      if (!responseReceived) {
        clientAborted = true;
        upstreamReq.destroy(err);
      }
    };

    let streamedRequestBytes = 0;
    const onRequestBodyChunk = (chunk: Buffer): void => {
      streamedRequestBytes += chunk.byteLength;
      if (streamedRequestBytes > config.limits.maxRequestBodyBytes) {
        const bodyErr = new ProxyError(
          413,
          'PAYLOAD_TOO_LARGE',
          `Request body exceeded limit of ${config.limits.maxRequestBodyBytes} bytes`,
        );
        req.unpipe(upstreamReq);
        upstreamReq.destroy(bodyErr);
      }
    };

    res.on('close', onClientClose);
    req.on('error', onClientReqError);
    req.on('data', onRequestBodyChunk);

    if (backpressureMetrics !== undefined) {
      upstreamReq.on('drain', () => {
        backpressureMetrics.upstreamPauseCount += 1;
      });
    }

    upstreamReq.on('error', (err) => {
      handleAttemptFailure(err);
    });

    upstreamReq.on('response', (upstreamRes: IncomingMessage) => {
      responseReceived = true;
      clearResponseTimeout();

      if (clientAborted || res.destroyed) {
        upstreamRes.resume();
        releaseConnectionOnce();
        cleanupListeners();
        return;
      }

      const statusCode = upstreamRes.statusCode ?? 502;
      ctx.statusCode = statusCode;

      if (statusCode < 500) {
        pool.recordLiveSuccess(upstream);
      }

      const draining = isDraining !== undefined ? isDraining() : false;
      if (draining) {
        res.shouldKeepAlive = false;
      }

      const responseHeaders = buildClientResponseHeaders(
        upstreamRes.headers,
        ctx.requestId,
        draining,
      );

      if (
        upstreamRes.statusMessage !== undefined &&
        upstreamRes.statusMessage.length > 0
      ) {
        res.writeHead(statusCode, upstreamRes.statusMessage, responseHeaders);
      } else {
        res.writeHead(statusCode, responseHeaders);
      }

      if (backpressureMetrics !== undefined) {
        res.on('drain', () => {
          backpressureMetrics.clientPauseCount += 1;
        });
      }

      pipeline(upstreamRes, res, (err) => {
        releaseConnectionOnce();
        cleanupListeners();
        if (err !== null && !clientAborted) {
          handleAttemptFailure(err);
        }
      });
    });

    // For GET/HEAD/OPTIONS there is no body to stream; calling end() directly
    // allows safe retries if the first upstream refuses the connection.
    if (canRetry) {
      req.resume();
      upstreamReq.end();
    } else {
      req.pipe(upstreamReq);
    }
  };

  executeAttempt();
}
