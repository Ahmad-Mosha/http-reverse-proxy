import type {
  LoadBalancingStrategy,
  PartialProxyConfig,
  PoolConfig,
  ProxyConfig,
  StreamConfig,
  TimeoutConfig,
  UpstreamConfig,
} from '../types/config.js';

export class ConfigValidationError extends Error {
  public constructor(message: string) {
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
};

export const DEFAULT_STREAM_CONFIG: StreamConfig = {
  highWaterMarkBytes: 64 * 1024, // 64 KiB
};

const VALID_STRATEGIES = new Set<LoadBalancingStrategy>([
  'round-robin',
  'least-connections',
]);

function normalizeHostForComparison(host: string): string {
  const lower = host.trim().toLowerCase();
  if (lower === 'localhost' || lower === '0.0.0.0' || lower === '::1' || lower === '::') {
    return '127.0.0.1';
  }
  return lower;
}

function assertPositiveInteger(value: number, fieldName: string): void {
  if (!Number.isInteger(value) || value <= 0) {
    throw new ConfigValidationError(
      `"${fieldName}" must be a positive integer, got ${value}`,
    );
  }
}

function validatePort(port: number, fieldName: string, allowZero: boolean): void {
  const min = allowZero ? 0 : 1;
  if (!Number.isInteger(port) || port < min || port > 65535) {
    throw new ConfigValidationError(
      `"${fieldName}" must be an integer between ${min} and 65535, got ${port}`,
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

  const seenIds = new Set<string>();
  const seenEndpoints = new Set<string>();
  const normalizedProxyHost = normalizeHostForComparison(host);

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

      const normalizedUpstreamHost = normalizeHostForComparison(upstreamHost);
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

  assertPositiveInteger(pool.keepAliveMsecs, 'pool.keepAliveMsecs');
  assertPositiveInteger(pool.maxSocketsPerUpstream, 'pool.maxSocketsPerUpstream');
  if (
    !Number.isInteger(pool.maxFreeSocketsPerUpstream) ||
    pool.maxFreeSocketsPerUpstream < 0
  ) {
    throw new ConfigValidationError(
      `"pool.maxFreeSocketsPerUpstream" must be a non-negative integer, got ${pool.maxFreeSocketsPerUpstream}`,
    );
  }
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
  };

  assertPositiveInteger(
    timeouts.upstreamResponseTimeoutMs,
    'timeouts.upstreamResponseTimeoutMs',
  );
  assertPositiveInteger(
    timeouts.upstreamSocketTimeoutMs,
    'timeouts.upstreamSocketTimeoutMs',
  );
  assertPositiveInteger(
    timeouts.clientHeadersTimeoutMs,
    'timeouts.clientHeadersTimeoutMs',
  );
  assertPositiveInteger(
    timeouts.clientRequestTimeoutMs,
    'timeouts.clientRequestTimeoutMs',
  );
  assertPositiveInteger(
    timeouts.clientKeepAliveTimeoutMs,
    'timeouts.clientKeepAliveTimeoutMs',
  );

  const stream: StreamConfig = {
    highWaterMarkBytes:
      input.stream?.highWaterMarkBytes ?? DEFAULT_STREAM_CONFIG.highWaterMarkBytes,
  };
  assertPositiveInteger(stream.highWaterMarkBytes, 'stream.highWaterMarkBytes');

  return {
    host,
    port,
    strategy,
    upstreams,
    pool,
    timeouts,
    stream,
    trustProxy: input.trustProxy ?? false,
  };
}
