import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
  DEFAULT_POOL_CONFIG,
  DEFAULT_TIMEOUT_CONFIG,
} from '../../src/config/validator.js';
import { ProxyError } from '../../src/http/errors.js';
import { LoadBalancer } from '../../src/pool/load-balancer.js';
import { UpstreamPool } from '../../src/pool/upstream-pool.js';

function createTestPool(): UpstreamPool {
  return new UpstreamPool(
    [
      { id: 'u1', host: '127.0.0.1', port: 9101 },
      { id: 'u2', host: '127.0.0.1', port: 9102 },
      { id: 'u3', host: '127.0.0.1', port: 9103 },
    ],
    DEFAULT_POOL_CONFIG,
    DEFAULT_TIMEOUT_CONFIG,
  );
}

describe('LoadBalancer', () => {
  it('distributes requests sequentially in round-robin order and skips unhealthy upstreams', () => {
    const pool = createTestPool();
    try {
      const lb = new LoadBalancer(pool, 'round-robin');

      const seq1 = [
        lb.selectUpstream().id,
        lb.selectUpstream().id,
        lb.selectUpstream().id,
        lb.selectUpstream().id,
        lb.selectUpstream().id,
        lb.selectUpstream().id,
      ];
      assert.deepEqual(seq1, ['u1', 'u2', 'u3', 'u1', 'u2', 'u3']);

      // Mark u2 as unhealthy
      pool.setUpstreamHealth('u2', false);

      const seq2 = [
        lb.selectUpstream().id,
        lb.selectUpstream().id,
        lb.selectUpstream().id,
        lb.selectUpstream().id,
      ];
      assert.deepEqual(seq2, ['u1', 'u3', 'u1', 'u3']);
    } finally {
      pool.destroy();
    }
  });

  it('selects upstream with fewest active connections and breaks ties fairly', () => {
    const pool = createTestPool();
    try {
      const lb = new LoadBalancer(pool, 'least-connections');

      // When all upstreams have 0 active connections, tie-breaking rotates fairly
      const initialPicks = [
        lb.selectUpstream().id,
        lb.selectUpstream().id,
        lb.selectUpstream().id,
      ];
      assert.deepEqual(new Set(initialPicks), new Set(['u1', 'u2', 'u3']));

      const u1 = pool.getUpstreamById('u1');
      const u2 = pool.getUpstreamById('u2');
      const u3 = pool.getUpstreamById('u3');
      assert.ok(u1 && u2 && u3);

      // Simulate asymmetric active load: u1 has 5, u2 has 2, u3 has 0
      u1.activeConnections = 5;
      u2.activeConnections = 2;
      u3.activeConnections = 0;

      assert.equal(lb.selectUpstream().id, 'u3');

      // Now u3 has 3 active connections -> u2 (with 2) should be selected next
      u3.activeConnections = 3;
      assert.equal(lb.selectUpstream().id, 'u2');
    } finally {
      pool.destroy();
    }
  });

  it('throws 503 NO_HEALTHY_UPSTREAM when all upstreams are unhealthy', () => {
    const pool = createTestPool();
    try {
      pool.setUpstreamHealth('u1', false);
      pool.setUpstreamHealth('u2', false);
      pool.setUpstreamHealth('u3', false);

      const lb = new LoadBalancer(pool, 'round-robin');
      assert.throws(
        () => lb.selectUpstream(),
        (err: unknown) =>
          err instanceof ProxyError &&
          err.statusCode === 503 &&
          err.code === 'NO_HEALTHY_UPSTREAM',
      );
    } finally {
      pool.destroy();
    }
  });
});
