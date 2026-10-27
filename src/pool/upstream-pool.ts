import { Agent } from 'node:http';
import type { PoolConfig, TimeoutConfig, UpstreamConfig } from '../types/config.js';
import type { UpstreamSnapshot, UpstreamTarget } from '../types/upstream.js';

export class UpstreamPool {
  private readonly targets: UpstreamTarget[];
  private readonly targetsById: Map<string, UpstreamTarget>;

  public constructor(
    upstreams: readonly UpstreamConfig[],
    poolConfig: PoolConfig,
    timeoutConfig: TimeoutConfig,
  ) {
    this.targets = upstreams.map((cfg) => {
      const agent = new Agent({
        keepAlive: poolConfig.keepAlive,
        keepAliveMsecs: poolConfig.keepAliveMsecs,
        maxSockets: poolConfig.maxSocketsPerUpstream,
        maxFreeSockets: poolConfig.maxFreeSocketsPerUpstream,
        scheduling: poolConfig.scheduling,
        timeout: timeoutConfig.upstreamSocketTimeoutMs,
      });

      return {
        id: cfg.id,
        host: cfg.host,
        port: cfg.port,
        protocol: cfg.protocol ?? 'http:',
        healthy: true,
        activeConnections: 0,
        totalRequests: 0,
        totalErrors: 0,
        agent,
      };
    });

    this.targetsById = new Map(this.targets.map((t) => [t.id, t]));
  }

  public getAllUpstreams(): readonly UpstreamTarget[] {
    return this.targets;
  }

  public getHealthyUpstreams(): UpstreamTarget[] {
    return this.targets.filter((t) => t.healthy);
  }

  public getUpstreamById(id: string): UpstreamTarget | undefined {
    return this.targetsById.get(id);
  }

  public setUpstreamHealth(id: string, healthy: boolean): void {
    const target = this.targetsById.get(id);
    if (target !== undefined) {
      target.healthy = healthy;
    }
  }

  public acquireConnection(target: UpstreamTarget): void {
    target.activeConnections += 1;
    target.totalRequests += 1;
  }

  public releaseConnection(target: UpstreamTarget): void {
    if (target.activeConnections > 0) {
      target.activeConnections -= 1;
    }
  }

  public recordError(target: UpstreamTarget): void {
    target.totalErrors += 1;
  }

  public getSnapshots(): UpstreamSnapshot[] {
    return this.targets.map((t) => ({
      id: t.id,
      host: t.host,
      port: t.port,
      healthy: t.healthy,
      activeConnections: t.activeConnections,
      totalRequests: t.totalRequests,
      totalErrors: t.totalErrors,
    }));
  }

  /**
   * Destroys all underlying HTTP Agents and closes any pooled idle Keep-Alive sockets.
   */
  public destroy(): void {
    for (const target of this.targets) {
      target.agent.destroy();
    }
  }
}
