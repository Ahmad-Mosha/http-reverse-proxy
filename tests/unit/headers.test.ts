import assert from 'node:assert/strict';
import { Agent } from 'node:http';
import { describe, it } from 'node:test';
import {
  buildClientResponseHeaders,
  buildUpstreamRequestHeaders,
  parseConnectionHeaderTokens,
} from '../../src/http/headers.js';
import type { UpstreamTarget } from '../../src/types/upstream.js';

function createDummyUpstream(): UpstreamTarget {
  const agent = new Agent();
  return {
    id: 'u1',
    host: '10.0.0.12',
    port: 4000,
    protocol: 'http:',
    healthy: true,
    activeConnections: 0,
    totalRequests: 0,
    totalErrors: 0,
    agent,
  };
}

describe('HTTP/1.1 Header Sanitization & Forwarding Semantics', () => {
  it('parses comma-separated tokens from Connection header case-insensitively', () => {
    const tokens = parseConnectionHeaderTokens(
      'keep-alive, X-Custom-Secret , x-internal-token',
    );
    assert.equal(tokens.has('keep-alive'), true);
    assert.equal(tokens.has('x-custom-secret'), true);
    assert.equal(tokens.has('x-internal-token'), true);
  });

  it('strips static and Connection-nominated hop-by-hop headers while preserving end-to-end headers', () => {
    const upstream = createDummyUpstream();
    try {
      const headers = buildUpstreamRequestHeaders({
        incomingHeaders: {
          host: 'api.example.com',
          'user-agent': 'RelayEdgeTest/1.0',
          accept: 'application/json',
          connection: 'keep-alive, x-internal-auth',
          'keep-alive': 'timeout=5',
          'transfer-encoding': 'chunked',
          te: 'trailers',
          upgrade: 'websocket',
          'x-internal-auth': 'secret-token-should-be-stripped',
        },
        upstream,
        clientIp: '192.0.2.55',
        requestId: 'req-123',
        trustProxy: false,
      });

      assert.equal(headers.host, '10.0.0.12:4000');
      assert.equal(headers['user-agent'], 'RelayEdgeTest/1.0');
      assert.equal(headers.accept, 'application/json');
      assert.equal(headers.connection, undefined);
      assert.equal(headers['keep-alive'], undefined);
      assert.equal(headers['transfer-encoding'], undefined);
      assert.equal(headers.te, undefined);
      assert.equal(headers.upgrade, undefined);
      assert.equal(headers['x-internal-auth'], undefined);
      assert.equal(headers['x-forwarded-for'], '192.0.2.55');
      assert.equal(headers['x-forwarded-host'], 'api.example.com');
      assert.equal(headers['x-forwarded-proto'], 'http');
      assert.equal(headers['x-request-id'], 'req-123');
      assert.equal(headers.via, '1.1 relay-edge');
    } finally {
      upstream.agent.destroy();
    }
  });

  it('prevents malicious Connection header from stripping proxy security headers (X-Forwarded-For / Host)', () => {
    const upstream = createDummyUpstream();
    try {
      const headers = buildUpstreamRequestHeaders({
        incomingHeaders: {
          host: 'public.example.com',
          connection: 'x-forwarded-for, x-request-id, host, via',
          'x-forwarded-for': '1.2.3.4',
        },
        upstream,
        clientIp: '203.0.113.9',
        requestId: 'req-sec-1',
        trustProxy: false,
      });

      assert.equal(headers.host, '10.0.0.12:4000');
      assert.equal(headers['x-forwarded-for'], '203.0.113.9');
      assert.equal(headers['x-request-id'], 'req-sec-1');
      assert.equal(headers.via, '1.1 relay-edge');
    } finally {
      upstream.agent.destroy();
    }
  });

  it('appends X-Forwarded-For when trustProxy is true', () => {
    const upstream = createDummyUpstream();
    try {
      const headers = buildUpstreamRequestHeaders({
        incomingHeaders: {
          host: 'public.example.com',
          'x-forwarded-for': '198.51.100.1',
        },
        upstream,
        clientIp: '203.0.113.9',
        requestId: 'req-trust-1',
        trustProxy: true,
      });

      assert.equal(headers['x-forwarded-for'], '198.51.100.1, 203.0.113.9');
    } finally {
      upstream.agent.destroy();
    }
  });

  it('sanitizes upstream response headers before forwarding to client', () => {
    const resHeaders = buildClientResponseHeaders(
      {
        'content-type': 'application/json',
        connection: 'close, x-upstream-private',
        'keep-alive': 'timeout=5',
        'x-upstream-private': 'hidden',
        'x-custom-header': 'visible',
      },
      'req-res-1',
    );

    assert.equal(resHeaders['content-type'], 'application/json');
    assert.equal(resHeaders['x-custom-header'], 'visible');
    assert.equal(resHeaders.connection, undefined);
    assert.equal(resHeaders['keep-alive'], undefined);
    assert.equal(resHeaders['x-upstream-private'], undefined);
    assert.equal(resHeaders['x-request-id'], 'req-res-1');
    assert.equal(resHeaders.via, '1.1 relay-edge');
  });
});
