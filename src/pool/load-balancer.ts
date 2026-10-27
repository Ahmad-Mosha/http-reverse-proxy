import { ProxyError } from '../http/errors.js';
import type { LoadBalancingStrategy } from '../types/config.js';
import type { UpstreamTarget } from '../types/upstream.js';
import type { UpstreamPool } from './upstream-pool.js';

export class LoadBalancer {
  private readonly pool: UpstreamPool;
  private readonly strategy: LoadBalancingStrategy;
  private readonly maxPerUpstream: number;
  private cursor = 0;

  constructor(
    pool: UpstreamPool,
    strategy: LoadBalancingStrategy,
    maxPerUpstream = Number.MAX_SAFE_INTEGER,
  ) {
    this.pool = pool;
    this.strategy = strategy;
    this.maxPerUpstream = maxPerUpstream;
  }

  getStrategy(): LoadBalancingStrategy {
    return this.strategy;
  }

  selectUpstream(excludedIds?: ReadonlySet<string>): UpstreamTarget {
    const healthy = this.pool
      .getHealthyUpstreams()
      .filter((t) => excludedIds === undefined || !excludedIds.has(t.id));

    if (healthy.length === 0) {
      throw new ProxyError(
        503,
        'NO_HEALTHY_UPSTREAM',
        'No healthy upstream servers are currently available to handle the request',
      );
    }

    const available = healthy.filter((t) => t.activeConnections < this.maxPerUpstream);
    const count = available.length;

    if (count === 0) {
      throw new ProxyError(
        503,
        'UPSTREAM_CAPACITY_EXCEEDED',
        'All healthy upstreams have reached maximum concurrent connection capacity',
      );
    }

    const startOffset = this.cursor;
    this.cursor = (this.cursor + 1) >>> 0;

    if (this.strategy === 'round-robin') {
      const selected = available[startOffset % count];
      if (selected === undefined) {
        throw new ProxyError(
          503,
          'NO_HEALTHY_UPSTREAM',
          'Failed to select healthy upstream',
        );
      }
      return selected;
    }

    let bestTarget = available[startOffset % count];
    if (bestTarget === undefined) {
      throw new ProxyError(
        503,
        'NO_HEALTHY_UPSTREAM',
        'Failed to select healthy upstream',
      );
    }

    for (let i = 1; i < count; i += 1) {
      const candidate = available[(startOffset + i) % count];
      if (
        candidate !== undefined &&
        candidate.activeConnections < bestTarget.activeConnections
      ) {
        bestTarget = candidate;
      }
    }

    return bestTarget;
  }
}
