import type { IncomingHttpHeaders, OutgoingHttpHeaders } from 'node:http';
import type { UpstreamTarget } from '../types/upstream.js';

// RFC 9110 Section 7.6.1 hop-by-hop headers scoped strictly to a single TCP link
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

const EMPTY_TOKEN_SET: ReadonlySet<string> = new Set<string>();

export function parseConnectionHeaderTokens(
  connectionHeader: string | string[] | undefined,
): ReadonlySet<string> {
  if (connectionHeader === undefined) {
    return EMPTY_TOKEN_SET;
  }

  // Fast path: standard single-token Connection values need no dynamic Set allocation
  if (typeof connectionHeader === 'string' && !connectionHeader.includes(',')) {
    const single = connectionHeader.trim().toLowerCase();
    if (single.length === 0 || single === 'keep-alive' || single === 'close') {
      return EMPTY_TOKEN_SET;
    }
    return new Set([single]);
  }

  const tokens = new Set<string>();
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
  incomingHeaders: IncomingHttpHeaders;
  upstream: UpstreamTarget;
  clientIp: string;
  requestId: string;
  trustProxy: boolean;
}

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

  const upstreamAuthority = `${upstream.host}:${upstream.port}`;
  const originalHost =
    typeof incomingHeaders.host === 'string' && incomingHeaders.host.trim().length > 0
      ? incomingHeaders.host.trim()
      : upstreamAuthority;
  sanitized.host = upstreamAuthority;

  // Forwarding headers are set after hop-by-hop removal so a client cannot strip
  // X-Forwarded-For or Host by nominating them inside `Connection: ...`
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

export function buildClientResponseHeaders(
  upstreamHeaders: IncomingHttpHeaders,
  requestId: string,
  draining = false,
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

  if (draining) {
    sanitized.connection = 'close';
  }

  return sanitized;
}
