import type { IncomingMessage, ServerResponse } from 'node:http';

export type ProxyErrorCode =
  | 'BAD_REQUEST'
  | 'METHOD_NOT_ALLOWED'
  | 'PAYLOAD_TOO_LARGE'
  | 'OVERLOADED'
  | 'SERVER_SHUTTING_DOWN'
  | 'NO_HEALTHY_UPSTREAM'
  | 'UPSTREAM_CAPACITY_EXCEEDED'
  | 'UPSTREAM_CONNECT_ERROR'
  | 'UPSTREAM_TIMEOUT'
  | 'UPSTREAM_PREMATURE_CLOSE'
  | 'UPSTREAM_PROTOCOL_ERROR'
  | 'INTERNAL_PROXY_ERROR';

export class ProxyError extends Error {
  readonly statusCode: number;
  readonly code: ProxyErrorCode;

  constructor(
    statusCode: number,
    code: ProxyErrorCode,
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
    this.name = 'ProxyError';
    this.statusCode = statusCode;
    this.code = code;
  }
}

const DISALLOWED_METHODS = new Set<string>(['CONNECT', 'TRACE']);
const SAFE_RETRY_METHODS = new Set<string>(['GET', 'HEAD', 'OPTIONS']);

export function isRetryableMethod(method: string | undefined): boolean {
  return SAFE_RETRY_METHODS.has((method ?? 'GET').toUpperCase());
}

export function validateIncomingRequest(
  req: IncomingMessage,
  maxBodyBytes?: number,
): void {
  const method = (req.method ?? 'GET').toUpperCase();
  if (DISALLOWED_METHODS.has(method)) {
    throw new ProxyError(
      405,
      'METHOD_NOT_ALLOWED',
      `HTTP method "${method}" is not allowed on this reverse proxy`,
    );
  }

  const url = req.url ?? '';
  if (!url.startsWith('/') || url.startsWith('//')) {
    throw new ProxyError(
      400,
      'BAD_REQUEST',
      'Only origin-form request targets starting with a single "/" are supported',
    );
  }

  // RFC 9112 Section 6.3: reject conflicting Content-Length and Transfer-Encoding
  if (
    req.headers['transfer-encoding'] !== undefined &&
    req.headers['content-length'] !== undefined
  ) {
    throw new ProxyError(
      400,
      'BAD_REQUEST',
      'Conflicting Content-Length and Transfer-Encoding headers are not permitted',
    );
  }

  if (maxBodyBytes !== undefined && req.headers['content-length'] !== undefined) {
    const contentLength = Number(req.headers['content-length']);
    if (!Number.isFinite(contentLength) || contentLength < 0) {
      throw new ProxyError(400, 'BAD_REQUEST', 'Invalid Content-Length header');
    }
    if (contentLength > maxBodyBytes) {
      throw new ProxyError(
        413,
        'PAYLOAD_TOO_LARGE',
        `Request body exceeds maximum allowed size of ${maxBodyBytes} bytes`,
      );
    }
  }
}

export function classifyUpstreamError(err: unknown, upstreamId: string): ProxyError {
  if (err instanceof ProxyError) {
    return err;
  }

  const nodeErr = err as NodeJS.ErrnoException | undefined;
  const code = nodeErr?.code;
  const message = nodeErr?.message ?? 'Unknown upstream error';

  if (code === 'ETIMEDOUT' || code === 'UND_ERR_HEADERS_TIMEOUT') {
    return new ProxyError(
      504,
      'UPSTREAM_TIMEOUT',
      `Upstream "${upstreamId}" timed out: ${message}`,
      { cause: err },
    );
  }

  if (
    code === 'ECONNRESET' ||
    code === 'EPIPE' ||
    message.toLowerCase().includes('premature close') ||
    message.toLowerCase().includes('socket hang up')
  ) {
    return new ProxyError(
      502,
      'UPSTREAM_PREMATURE_CLOSE',
      `Upstream "${upstreamId}" closed connection prematurely: ${message}`,
      { cause: err },
    );
  }

  if (code?.startsWith('HPE_')) {
    return new ProxyError(
      502,
      'UPSTREAM_PROTOCOL_ERROR',
      `Upstream "${upstreamId}" returned malformed HTTP response (${code})`,
      { cause: err },
    );
  }

  return new ProxyError(
    502,
    'UPSTREAM_CONNECT_ERROR',
    `Failed to communicate with upstream "${upstreamId}": ${message}`,
    { cause: err },
  );
}

export function sendProxyError(
  res: ServerResponse,
  error: ProxyError,
  requestId: string,
  closeConnection = false,
): void {
  if (res.destroyed || res.writableEnded) {
    return;
  }

  // Once headers are flushed on the wire, we cannot send a new HTTP status line;
  // destroy the socket so the client sees an abnormal termination instead of truncated data.
  if (res.headersSent) {
    res.destroy(error);
    return;
  }

  const payload = JSON.stringify({
    error: {
      code: error.code,
      message: error.message,
      statusCode: error.statusCode,
      requestId,
    },
  });

  const bodyBuffer = Buffer.from(payload, 'utf8');
  const headers: Record<string, string | number> = {
    'content-type': 'application/json; charset=utf-8',
    'content-length': bodyBuffer.byteLength,
    'cache-control': 'no-store',
    'x-request-id': requestId,
    via: '1.1 relay-edge',
  };

  if (closeConnection) {
    headers['connection'] = 'close';
  }

  res.writeHead(error.statusCode, headers);
  res.end(bodyBuffer);
}
