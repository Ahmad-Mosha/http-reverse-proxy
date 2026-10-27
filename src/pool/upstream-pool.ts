import { Agent } from 'node:http';
import type {
  HealthCheckConfig,
  PoolConfig,
  TimeoutConfig,
  UpstreamConfig,
} from '../types/config.js';
import type { UpstreamSnapshot, UpstreamTarget } from '../types/upstream.js';
import { DEFAULT_HEALTH_CHECK_CONFIG } from '../config/validator.js';

export class UpstreamPool {
  private readonly targets: UpstreamTarget[];
  private readonly targetsById: Map<string, UpstreamTarget>;
  private readonly healthConfig: HealthCheckConfig;

  constructor(
    upstreams: readonly UpstreamConfig[],
    poolConfig: PoolConfig,
    timeoutConfig: TimeoutConfig,
    healthConfig: HealthCheckConfig = DEFAULT_HEALTH_CHECK_CONFIG,
  ) {
    this.healthConfig = healthConfig;
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
        healthCheckPath: cfg.healthCheckPath ?? healthConfig.path,
        healthy: true,
        activeConnections: 0,
        consecutiveFailures: 0,
        consecutiveSuccesses: 0,
        totalRequests: 0,
        totalErrors: 0,
        agent,
      };
    });

    this.targetsById = new Map(this.targets.map((t) => [t.id, t]));
  }

  getAllUpstreams(): readonly UpstreamTarget[] {
    return this.targets;
  }

  getHealthyUpstreams(): UpstreamTarget[] {
    return this.targets.filter((t) => t.healthy);
  }

  getUpstreamById(id: string): UpstreamTarget | undefined {
    return this.targetsById.get(id);
  }

  setUpstreamHealth(id: string, healthy: boolean): void {
    const target = this.targetsById.get(id);
    if (target !== undefined) {
      target.healthy = healthy;
      if (healthy) {
        target.consecutiveFailures = 0;
      } else {
        target.consecutiveSuccesses = 0;
      }
    }
  }

  acquireConnection(target: UpstreamTarget): void {
    target.activeConnections += 1;
    target.totalRequests += 1;
  }

  releaseConnection(target: UpstreamTarget): void {
    if (target.activeConnections > 0) {
      target.activeConnections -= 1;
    }
  }

  recordError(target: UpstreamTarget): void {
    target.totalErrors += 1;
    target.consecutiveFailures += 1;
    target.consecutiveSuccesses = 0;

    if (
      target.healthy &&
      target.consecutiveFailures >= this.healthConfig.unhealthyThreshold
    ) {
      target.healthy = false;
    }
  }

  recordLiveSuccess(target: UpstreamTarget): void {
    target.consecutiveFailures = 0;
  }

  recordProbeResult(target: UpstreamTarget, ok: boolean): void {
    if (ok) {
      target.consecutiveSuccesses += 1;
      target.consecutiveFailures = 0;
      if (
        !target.healthy &&
        target.consecutiveSuccesses >= this.healthConfig.healthyThreshold
      ) {
        target.healthy = true;
      }
    } else {
      target.consecutiveFailures += 1;
      target.consecutiveSuccesses = 0;
      if (
        target.healthy &&
        target.consecutiveFailures >= this.healthConfig.unhealthyThreshold
      ) {
        target.healthy = false;
      }
    }
  }

  getSnapshots(): UpstreamSnapshot[] {
    return this.targets.map((t) => ({
      id: t.id,
      host: t.host,
      port: t.port,
      healthy: t.healthy,
      activeConnections: t.activeConnections,
      consecutiveFailures: t.consecutiveFailures,
      totalRequests: t.totalRequests,
      totalErrors: t.totalErrors,
    }));
  }

  destroy(): void {
    for (const target of this.targets) {
      target.agent.destroy();
    }
  }
}
