import type { LogLevel } from '../observability/logger.js';
import type {
  HealthCheckConfig,
  LimitsConfig,
  LoadBalancingStrategy,
  ObservabilityConfig,
  PartialProxyConfig,
  PoolConfig,
  ProxyConfig,
  StreamConfig,
  TimeoutConfig,
  UpstreamConfig,
} from '../types/config.js';

export class ConfigValidationError extends Error {
  constructor(message: string) {
    super(`Invalid RelayEdge configuration: ${message}`);
    this.name = 'ConfigValidationError';
  }
}

export const DEFAULT_POOL_CONFIG: PoolConfig = {
  keepAlive: true,
  keepAliveMsecs: 1_000,
  maxSocketsPerUpstream: 128,
  maxFreeSocketsPerUpstream: 32,
  scheduling: 'lifo',
};

export const DEFAULT_TIMEOUT_CONFIG: TimeoutConfig = {
  upstreamResponseTimeoutMs: 15_000,
  upstreamSocketTimeoutMs: 30_000,
  clientHeadersTimeoutMs: 10_000,
  clientRequestTimeoutMs: 30_000,
  clientKeepAliveTimeoutMs: 5_000,
  shutdownGracePeriodMs: 10_000,
};

export const DEFAULT_STREAM_CONFIG: StreamConfig = {
  highWaterMarkBytes: 64 * 1024,
};

export const DEFAULT_HEALTH_CHECK_CONFIG: HealthCheckConfig = {
  enabled: false,
  path: '/healthz',
  intervalMs: 5_000,
  timeoutMs: 2_000,
  unhealthyThreshold: 3,
  healthyThreshold: 2,
};

export const DEFAULT_LIMITS_CONFIG: LimitsConfig = {
  maxActiveRequests: 2_048,
  maxActiveRequestsPerUpstream: 512,
  maxRequestBodyBytes: 10 * 1024 * 1024,
  maxRetries: 1,
};

export const DEFAULT_OBSERVABILITY_CONFIG: ObservabilityConfig = {
  metricsPath: '/__relay_edge/metrics',
  healthPath: '/__relay_edge/healthz',
  logLevel: 'silent',
};

const VALID_STRATEGIES = new Set<LoadBalancingStrategy>([
  'round-robin',
  'least-connections',
]);

const VALID_LOG_LEVELS = new Set<LogLevel>(['debug', 'info', 'warn', 'error', 'silent']);

function normalizeHost(host: string): string {
  const lower = host.trim().toLowerCase();
  if (lower === 'localhost' || lower === '0.0.0.0' || lower === '::1' || lower === '::') {
    return '127.0.0.1';
  }
  return lower;
}

function assertPositiveInt(value: number, field: string): void {
  if (!Number.isInteger(value) || value <= 0) {
    throw new ConfigValidationError(
      `"${field}" must be a positive integer, got ${value}`,
    );
  }
}

function assertNonNegativeInt(value: number, field: string): void {
  if (!Number.isInteger(value) || value < 0) {
    throw new ConfigValidationError(
      `"${field}" must be a non-negative integer, got ${value}`,
    );
  }
}

function validatePort(port: number, field: string, allowZero: boolean): void {
  const min = allowZero ? 0 : 1;
  if (!Number.isInteger(port) || port < min || port > 65535) {
    throw new ConfigValidationError(
      `"${field}" must be an integer between ${min} and 65535, got ${port}`,
    );
  }
}

export function createProxyConfig(input: PartialProxyConfig): ProxyConfig {
  const host = (input.host ?? '127.0.0.1').trim();
  if (host.length === 0) {
    throw new ConfigValidationError('"host" must be a non-empty string');
  }

  const port = input.port ?? 8080;
  validatePort(port, 'port', true);

  const strategy = input.strategy ?? 'round-robin';
  if (!VALID_STRATEGIES.has(strategy)) {
    throw new ConfigValidationError(
      `"strategy" must be "round-robin" or "least-connections", got "${strategy}"`,
    );
  }

  if (input.upstreams.length === 0) {
    throw new ConfigValidationError(
      '"upstreams" must contain at least one upstream target',
    );
  }

  const healthCheck: HealthCheckConfig = {
    enabled: input.healthCheck?.enabled ?? DEFAULT_HEALTH_CHECK_CONFIG.enabled,
    path: (input.healthCheck?.path ?? DEFAULT_HEALTH_CHECK_CONFIG.path).trim(),
    intervalMs: input.healthCheck?.intervalMs ?? DEFAULT_HEALTH_CHECK_CONFIG.intervalMs,
    timeoutMs: input.healthCheck?.timeoutMs ?? DEFAULT_HEALTH_CHECK_CONFIG.timeoutMs,
    unhealthyThreshold:
      input.healthCheck?.unhealthyThreshold ??
      DEFAULT_HEALTH_CHECK_CONFIG.unhealthyThreshold,
    healthyThreshold:
      input.healthCheck?.healthyThreshold ?? DEFAULT_HEALTH_CHECK_CONFIG.healthyThreshold,
  };

  if (!healthCheck.path.startsWith('/')) {
    throw new ConfigValidationError('"healthCheck.path" must start with "/"');
  }
  assertPositiveInt(healthCheck.intervalMs, 'healthCheck.intervalMs');
  assertPositiveInt(healthCheck.timeoutMs, 'healthCheck.timeoutMs');
  assertPositiveInt(healthCheck.unhealthyThreshold, 'healthCheck.unhealthyThreshold');
  assertPositiveInt(healthCheck.healthyThreshold, 'healthCheck.healthyThreshold');

  const seenIds = new Set<string>();
  const seenEndpoints = new Set<string>();
  const normalizedProxyHost = normalizeHost(host);

  const upstreams: UpstreamConfig[] = input.upstreams.map(
    (upstream: UpstreamConfig, index: number) => {
      const id = upstream.id.trim();
      if (id.length === 0) {
        throw new ConfigValidationError(
          `upstreams[${index}].id must be a non-empty string`,
        );
      }
      if (seenIds.has(id)) {
        throw new ConfigValidationError(`Duplicate upstream id "${id}"`);
      }
      seenIds.add(id);

      const upstreamHost = upstream.host.trim();
      if (upstreamHost.length === 0) {
        throw new ConfigValidationError(
          `upstreams[${index}].host must be a non-empty string`,
        );
      }

      validatePort(upstream.port, `upstreams[${index}].port`, false);

      const protocol: string = upstream.protocol ?? 'http:';
      if (protocol !== 'http:') {
        throw new ConfigValidationError(`upstreams[${index}].protocol must be "http:"`);
      }

      const hcPath = (upstream.healthCheckPath ?? healthCheck.path).trim();
      if (!hcPath.startsWith('/')) {
        throw new ConfigValidationError(
          `upstreams[${index}].healthCheckPath must start with "/"`,
        );
      }

      const normalizedUpstreamHost = normalizeHost(upstreamHost);
      if (
        port > 0 &&
        normalizedUpstreamHost === normalizedProxyHost &&
        upstream.port === port
      ) {
        throw new ConfigValidationError(
          `Upstream "${id}" (${upstreamHost}:${upstream.port}) points to the proxy's own listen address (${host}:${port}), which would cause an infinite forwarding loop`,
        );
      }

      const endpointKey = `${normalizedUpstreamHost}:${upstream.port}`;
      if (seenEndpoints.has(endpointKey)) {
        throw new ConfigValidationError(
          `Duplicate upstream endpoint "${upstreamHost}:${upstream.port}"`,
        );
      }
      seenEndpoints.add(endpointKey);

      return {
        id,
        host: upstreamHost,
        port: upstream.port,
        protocol: 'http:',
        healthCheckPath: hcPath,
      };
    },
  );

  const pool: PoolConfig = {
    keepAlive: input.pool?.keepAlive ?? DEFAULT_POOL_CONFIG.keepAlive,
    keepAliveMsecs: input.pool?.keepAliveMsecs ?? DEFAULT_POOL_CONFIG.keepAliveMsecs,
    maxSocketsPerUpstream:
      input.pool?.maxSocketsPerUpstream ?? DEFAULT_POOL_CONFIG.maxSocketsPerUpstream,
    maxFreeSocketsPerUpstream:
      input.pool?.maxFreeSocketsPerUpstream ??
      DEFAULT_POOL_CONFIG.maxFreeSocketsPerUpstream,
    scheduling: input.pool?.scheduling ?? DEFAULT_POOL_CONFIG.scheduling,
  };

  assertPositiveInt(pool.keepAliveMsecs, 'pool.keepAliveMsecs');
  assertPositiveInt(pool.maxSocketsPerUpstream, 'pool.maxSocketsPerUpstream');
  assertNonNegativeInt(pool.maxFreeSocketsPerUpstream, 'pool.maxFreeSocketsPerUpstream');

  const schedulingValue: string = pool.scheduling;
  if (schedulingValue !== 'lifo' && schedulingValue !== 'fifo') {
    throw new ConfigValidationError('"pool.scheduling" must be "lifo" or "fifo"');
  }

  const timeouts: TimeoutConfig = {
    upstreamResponseTimeoutMs:
      input.timeouts?.upstreamResponseTimeoutMs ??
      DEFAULT_TIMEOUT_CONFIG.upstreamResponseTimeoutMs,
    upstreamSocketTimeoutMs:
      input.timeouts?.upstreamSocketTimeoutMs ??
      DEFAULT_TIMEOUT_CONFIG.upstreamSocketTimeoutMs,
    clientHeadersTimeoutMs:
      input.timeouts?.clientHeadersTimeoutMs ??
      DEFAULT_TIMEOUT_CONFIG.clientHeadersTimeoutMs,
    clientRequestTimeoutMs:
      input.timeouts?.clientRequestTimeoutMs ??
      DEFAULT_TIMEOUT_CONFIG.clientRequestTimeoutMs,
    clientKeepAliveTimeoutMs:
      input.timeouts?.clientKeepAliveTimeoutMs ??
      DEFAULT_TIMEOUT_CONFIG.clientKeepAliveTimeoutMs,
    shutdownGracePeriodMs:
      input.timeouts?.shutdownGracePeriodMs ??
      DEFAULT_TIMEOUT_CONFIG.shutdownGracePeriodMs,
  };

  assertPositiveInt(
    timeouts.upstreamResponseTimeoutMs,
    'timeouts.upstreamResponseTimeoutMs',
  );
  assertPositiveInt(timeouts.upstreamSocketTimeoutMs, 'timeouts.upstreamSocketTimeoutMs');
  assertPositiveInt(timeouts.clientHeadersTimeoutMs, 'timeouts.clientHeadersTimeoutMs');
  assertPositiveInt(timeouts.clientRequestTimeoutMs, 'timeouts.clientRequestTimeoutMs');
  assertPositiveInt(
    timeouts.clientKeepAliveTimeoutMs,
    'timeouts.clientKeepAliveTimeoutMs',
  );
  assertPositiveInt(timeouts.shutdownGracePeriodMs, 'timeouts.shutdownGracePeriodMs');

  const stream: StreamConfig = {
    highWaterMarkBytes:
      input.stream?.highWaterMarkBytes ?? DEFAULT_STREAM_CONFIG.highWaterMarkBytes,
  };
  assertPositiveInt(stream.highWaterMarkBytes, 'stream.highWaterMarkBytes');

  const limits: LimitsConfig = {
    maxActiveRequests:
      input.limits?.maxActiveRequests ?? DEFAULT_LIMITS_CONFIG.maxActiveRequests,
    maxActiveRequestsPerUpstream:
      input.limits?.maxActiveRequestsPerUpstream ??
      DEFAULT_LIMITS_CONFIG.maxActiveRequestsPerUpstream,
    maxRequestBodyBytes:
      input.limits?.maxRequestBodyBytes ?? DEFAULT_LIMITS_CONFIG.maxRequestBodyBytes,
    maxRetries: input.limits?.maxRetries ?? DEFAULT_LIMITS_CONFIG.maxRetries,
  };

  assertPositiveInt(limits.maxActiveRequests, 'limits.maxActiveRequests');
  assertPositiveInt(
    limits.maxActiveRequestsPerUpstream,
    'limits.maxActiveRequestsPerUpstream',
  );
  assertPositiveInt(limits.maxRequestBodyBytes, 'limits.maxRequestBodyBytes');
  assertNonNegativeInt(limits.maxRetries, 'limits.maxRetries');

  const observability: ObservabilityConfig = {
    metricsPath: (
      input.observability?.metricsPath ?? DEFAULT_OBSERVABILITY_CONFIG.metricsPath
    ).trim(),
    healthPath: (
      input.observability?.healthPath ?? DEFAULT_OBSERVABILITY_CONFIG.healthPath
    ).trim(),
    logLevel: input.observability?.logLevel ?? DEFAULT_OBSERVABILITY_CONFIG.logLevel,
  };

  if (!observability.metricsPath.startsWith('/')) {
    throw new ConfigValidationError('"observability.metricsPath" must start with "/"');
  }
  if (!observability.healthPath.startsWith('/')) {
    throw new ConfigValidationError('"observability.healthPath" must start with "/"');
  }
  if (!VALID_LOG_LEVELS.has(observability.logLevel)) {
    throw new ConfigValidationError(
      `Invalid observability.logLevel "${observability.logLevel}"`,
    );
  }

  return {
    host,
    port,
    strategy,
    upstreams,
    pool,
    timeouts,
    stream,
    healthCheck,
    limits,
    observability,
    trustProxy: input.trustProxy ?? false,
  };
}
