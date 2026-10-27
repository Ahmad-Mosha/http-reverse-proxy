import { ProxyError } from '../http/errors.js';
import type { LoadBalancingStrategy } from '../types/config.js';
import type { UpstreamTarget } from '../types/upstream.js';
import type { UpstreamPool } from './upstream-pool.js';

export class LoadBalancer {
  private readonly pool: UpstreamPool;
  private readonly strategy: LoadBalancingStrategy;
  private cursor = 0;

  public constructor(pool: UpstreamPool, strategy: LoadBalancingStrategy) {
    this.pool = pool;
    this.strategy = strategy;
  }

  public getStrategy(): LoadBalancingStrategy {
    return this.strategy;
  }

  /**
   * Selects the next healthy upstream target according to the configured strategy.
   * Throws a 503 NO_HEALTHY_UPSTREAM ProxyError if all upstreams are unhealthy.
   */
  public selectUpstream(): UpstreamTarget {
    const healthy = this.pool.getHealthyUpstreams();
    const count = healthy.length;

    if (count === 0) {
      throw new ProxyError(
        503,
        'NO_HEALTHY_UPSTREAM',
        'No healthy upstream servers are currently available to handle the request',
      );
    }

    const startOffset = this.cursor;
    this.cursor = (this.cursor + 1) >>> 0;

    if (this.strategy === 'round-robin') {
      const selected = healthy[startOffset % count];
      if (selected === undefined) {
        throw new ProxyError(
          503,
          'NO_HEALTHY_UPSTREAM',
          'Failed to select healthy upstream',
        );
      }
      return selected;
    }

    // Least Connections with Round-Robin tie-breaking via rotating startOffset
    let bestTarget = healthy[startOffset % count];
    if (bestTarget === undefined) {
      throw new ProxyError(
        503,
        'NO_HEALTHY_UPSTREAM',
        'Failed to select healthy upstream',
      );
    }

    for (let i = 1; i < count; i += 1) {
      const candidate = healthy[(startOffset + i) % count];
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
