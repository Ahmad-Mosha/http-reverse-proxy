import type { LogLevel } from '../observability/logger.js';

export type LoadBalancingStrategy = 'round-robin' | 'least-connections';

export interface UpstreamConfig {
  id: string;
  host: string;
  port: number;
  protocol?: 'http:';
  healthCheckPath?: string;
}

export interface PoolConfig {
  keepAlive: boolean;
  keepAliveMsecs: number;
  maxSocketsPerUpstream: number;
  maxFreeSocketsPerUpstream: number;
  scheduling: 'lifo' | 'fifo';
}

export interface TimeoutConfig {
  upstreamResponseTimeoutMs: number;
  upstreamSocketTimeoutMs: number;
  clientHeadersTimeoutMs: number;
  clientRequestTimeoutMs: number;
  clientKeepAliveTimeoutMs: number;
  shutdownGracePeriodMs: number;
}

export interface StreamConfig {
  highWaterMarkBytes: number;
}

export interface HealthCheckConfig {
  enabled: boolean;
  path: string;
  intervalMs: number;
  timeoutMs: number;
  unhealthyThreshold: number;
  healthyThreshold: number;
}

export interface LimitsConfig {
  maxActiveRequests: number;
  maxActiveRequestsPerUpstream: number;
  maxRequestBodyBytes: number;
  maxRetries: number;
}

export interface ObservabilityConfig {
  metricsPath: string;
  healthPath: string;
  logLevel: LogLevel;
}

export interface ProxyConfig {
  host: string;
  port: number;
  strategy: LoadBalancingStrategy;
  upstreams: UpstreamConfig[];
  pool: PoolConfig;
  timeouts: TimeoutConfig;
  stream: StreamConfig;
  healthCheck: HealthCheckConfig;
  limits: LimitsConfig;
  observability: ObservabilityConfig;
  trustProxy: boolean;
}

export interface PartialProxyConfig {
  host?: string;
  port?: number;
  strategy?: LoadBalancingStrategy;
  upstreams: UpstreamConfig[];
  pool?: Partial<PoolConfig>;
  timeouts?: Partial<TimeoutConfig>;
  stream?: Partial<StreamConfig>;
  healthCheck?: Partial<HealthCheckConfig>;
  limits?: Partial<LimitsConfig>;
  observability?: Partial<ObservabilityConfig>;
  trustProxy?: boolean;
}
