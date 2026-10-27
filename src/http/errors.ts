import type { IncomingMessage, ServerResponse } from 'node:http';

export type ProxyErrorCode =
  | 'BAD_REQUEST'
  | 'METHOD_NOT_ALLOWED'
  | 'NO_HEALTHY_UPSTREAM'
  | 'UPSTREAM_CONNECT_ERROR'
  | 'UPSTREAM_TIMEOUT'
  | 'UPSTREAM_PREMATURE_CLOSE'
  | 'UPSTREAM_PROTOCOL_ERROR'
  | 'INTERNAL_PROXY_ERROR';

export class ProxyError extends Error {
  public readonly statusCode: number;
  public readonly code: ProxyErrorCode;

  public constructor(
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

/**
 * Validates that the incoming HTTP request conforms to safe reverse-proxy semantics
 * and rejects open-proxy / smuggling vectors before selecting an upstream.
 */
export function validateIncomingRequest(req: IncomingMessage): void {
  const method = (req.method ?? 'GET').toUpperCase();
  if (DISALLOWED_METHODS.has(method)) {
    throw new ProxyError(
      405,
      'METHOD_NOT_ALLOWED',
      `HTTP method "${method}" is not allowed on this reverse proxy`,
    );
  }

  const url = req.url ?? '';
  // Reject absolute-form (`http://...`), authority-form (`host:port`), or protocol-relative (`//...`) URLs
  if (!url.startsWith('/') || url.startsWith('//')) {
    throw new ProxyError(
      400,
      'BAD_REQUEST',
      'Only origin-form request targets starting with a single "/" are supported',
    );
  }

  // Defense-in-depth against CL.TE / TE.CL HTTP Request Smuggling (RFC 9112 Section 6.3)
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
}

/**
 * Maps low-level Node.js / libuv socket and HTTP parser errors from an upstream
 * into appropriate HTTP 502 / 504 ProxyErrors.
 */
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

/**
 * Safely writes an error response to the client if headers have not yet been sent.
 * If headers have already been flushed on the wire, destroys the socket immediately
 * so the client detects an abnormal termination rather than receiving corrupted payload bytes.
 */
export function sendProxyError(
  res: ServerResponse,
  error: ProxyError,
  requestId: string,
): void {
  if (res.destroyed || res.writableEnded) {
    return;
  }

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

  res.writeHead(error.statusCode, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': bodyBuffer.byteLength,
    'cache-control': 'no-store',
    'x-request-id': requestId,
    via: '1.1 relay-edge',
  });
  res.end(bodyBuffer);
}
