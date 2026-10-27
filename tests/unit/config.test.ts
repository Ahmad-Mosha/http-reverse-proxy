import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { ConfigValidationError, createProxyConfig } from '../../src/config/validator.js';

describe('Configuration Validator', () => {
  it('applies sensible defaults for a valid minimal configuration', () => {
    const config = createProxyConfig({
      port: 8080,
      upstreams: [
        { id: 'u1', host: '127.0.0.1', port: 9001 },
        { id: 'u2', host: '127.0.0.1', port: 9002 },
      ],
    });

    assert.equal(config.host, '127.0.0.1');
    assert.equal(config.port, 8080);
    assert.equal(config.strategy, 'round-robin');
    assert.equal(config.upstreams.length, 2);
    assert.equal(config.pool.keepAlive, true);
    assert.equal(config.pool.scheduling, 'lifo');
    assert.equal(config.trustProxy, false);
  });

  it('rejects empty upstreams array', () => {
    assert.throws(
      () =>
        createProxyConfig({
          upstreams: [],
        }),
      ConfigValidationError,
    );
  });

  it('rejects duplicate upstream IDs and duplicate endpoints', () => {
    assert.throws(
      () =>
        createProxyConfig({
          upstreams: [
            { id: 'u1', host: '127.0.0.1', port: 9001 },
            { id: 'u1', host: '127.0.0.1', port: 9002 },
          ],
        }),
      /Duplicate upstream id "u1"/,
    );

    assert.throws(
      () =>
        createProxyConfig({
          upstreams: [
            { id: 'u1', host: '127.0.0.1', port: 9001 },
            { id: 'u2', host: 'localhost', port: 9001 },
          ],
        }),
      /Duplicate upstream endpoint/,
    );
  });

  it('rejects self-proxying upstream loop', () => {
    assert.throws(
      () =>
        createProxyConfig({
          host: '127.0.0.1',
          port: 8080,
          upstreams: [{ id: 'self', host: 'localhost', port: 8080 }],
        }),
      /infinite forwarding loop/,
    );
  });

  it('rejects invalid ports and non-positive timeout values', () => {
    assert.throws(
      () =>
        createProxyConfig({
          port: 70000,
          upstreams: [{ id: 'u1', host: '127.0.0.1', port: 9001 }],
        }),
      ConfigValidationError,
    );

    assert.throws(
      () =>
        createProxyConfig({
          upstreams: [{ id: 'u1', host: '127.0.0.1', port: 9001 }],
          timeouts: { upstreamResponseTimeoutMs: 0 },
        }),
      ConfigValidationError,
    );
  });
});
