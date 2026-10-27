import type { IncomingHttpHeaders, OutgoingHttpHeaders } from 'node:http';
import type { UpstreamTarget } from '../types/upstream.js';

/**
 * Standard HTTP/1.1 hop-by-hop headers (RFC 9110 Section 7.6.1) that belong strictly
 * to a single TCP transport connection and must not be forwarded across proxy hops.
 */
const STATIC_HOP_BY_HOP_HEADERS = new Set<string>([
  'connection',
  'keep-alive',
  'proxy-authenticate',
  'proxy-authorization',
  'proxy-connection',
  'te',
  'trailer',
  'transfer-encoding',
  'upgrade',
]);

/**
 * Extracts header names dynamically nominated as hop-by-hop inside the `Connection` header.
 * Per RFC 9110 Section 7.6.1, any header field name listed in the Connection header
 * must be removed before forwarding the message.
 */
export function parseConnectionHeaderTokens(
  connectionHeader: string | string[] | undefined,
): Set<string> {
  const tokens = new Set<string>();
  if (connectionHeader === undefined) {
    return tokens;
  }

  const rawValues = Array.isArray(connectionHeader)
    ? connectionHeader
    : [connectionHeader];

  for (const raw of rawValues) {
    for (const part of raw.split(',')) {
      const token = part.trim().toLowerCase();
      if (token.length > 0) {
        tokens.add(token);
      }
    }
  }

  return tokens;
}

export interface BuildUpstreamRequestHeadersOptions {
  readonly incomingHeaders: IncomingHttpHeaders;
  readonly upstream: UpstreamTarget;
  readonly clientIp: string;
  readonly requestId: string;
  readonly trustProxy: boolean;
}

/**
 * Sanitizes incoming client headers and constructs safe end-to-end headers
 * for the outbound request to the selected upstream target.
 */
export function buildUpstreamRequestHeaders(
  options: BuildUpstreamRequestHeadersOptions,
): OutgoingHttpHeaders {
  const { incomingHeaders, upstream, clientIp, requestId, trustProxy } = options;
  const dynamicHopByHop = parseConnectionHeaderTokens(incomingHeaders.connection);
  const sanitized: OutgoingHttpHeaders = {};

  for (const [rawKey, value] of Object.entries(incomingHeaders)) {
    if (value === undefined) {
      continue;
    }
    const key = rawKey.toLowerCase();
    if (STATIC_HOP_BY_HOP_HEADERS.has(key) || dynamicHopByHop.has(key)) {
      continue;
    }
    sanitized[key] = value;
  }

  // Rewrite Host header to target upstream authority (RFC 9110 Section 7.2)
  const originalHost =
    typeof incomingHeaders.host === 'string' && incomingHeaders.host.trim().length > 0
      ? incomingHeaders.host.trim()
      : `${upstream.host}:${upstream.port}`;
  sanitized.host = `${upstream.host}:${upstream.port}`;

  // Set forwarding & identity headers AFTER hop-by-hop stripping so a client cannot
  // abuse `Connection: x-forwarded-for, x-request-id` to strip proxy-injected headers.
  if (
    trustProxy &&
    typeof incomingHeaders['x-forwarded-for'] === 'string' &&
    incomingHeaders['x-forwarded-for'].trim().length > 0
  ) {
    sanitized['x-forwarded-for'] =
      `${incomingHeaders['x-forwarded-for'].trim()}, ${clientIp}`;
  } else {
    sanitized['x-forwarded-for'] = clientIp;
  }

  if (
    trustProxy &&
    typeof incomingHeaders['x-forwarded-host'] === 'string' &&
    incomingHeaders['x-forwarded-host'].trim().length > 0
  ) {
    sanitized['x-forwarded-host'] = incomingHeaders['x-forwarded-host'].trim();
  } else {
    sanitized['x-forwarded-host'] = originalHost;
  }

  if (
    trustProxy &&
    typeof incomingHeaders['x-forwarded-proto'] === 'string' &&
    incomingHeaders['x-forwarded-proto'].trim().length > 0
  ) {
    sanitized['x-forwarded-proto'] = incomingHeaders['x-forwarded-proto'].trim();
  } else {
    sanitized['x-forwarded-proto'] = 'http';
  }

  sanitized['x-request-id'] = requestId;

  const existingVia =
    typeof incomingHeaders.via === 'string' && incomingHeaders.via.trim().length > 0
      ? `${incomingHeaders.via.trim()}, 1.1 relay-edge`
      : '1.1 relay-edge';
  sanitized.via = existingVia;

  return sanitized;
}

/**
 * Sanitizes upstream response headers before sending them back to the downstream client.
 */
export function buildClientResponseHeaders(
  upstreamHeaders: IncomingHttpHeaders,
  requestId: string,
): OutgoingHttpHeaders {
  const dynamicHopByHop = parseConnectionHeaderTokens(upstreamHeaders.connection);
  const sanitized: OutgoingHttpHeaders = {};

  for (const [rawKey, value] of Object.entries(upstreamHeaders)) {
    if (value === undefined) {
      continue;
    }
    const key = rawKey.toLowerCase();
    if (STATIC_HOP_BY_HOP_HEADERS.has(key) || dynamicHopByHop.has(key)) {
      continue;
    }
    sanitized[key] = value;
  }

  sanitized['x-request-id'] = requestId;
  const existingVia =
    typeof upstreamHeaders.via === 'string' && upstreamHeaders.via.trim().length > 0
      ? `${upstreamHeaders.via.trim()}, 1.1 relay-edge`
      : '1.1 relay-edge';
  sanitized.via = existingVia;

  return sanitized;
}
