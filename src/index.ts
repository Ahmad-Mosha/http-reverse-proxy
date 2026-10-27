export {
  ConfigValidationError,
  createProxyConfig,
  DEFAULT_HEALTH_CHECK_CONFIG,
  DEFAULT_LIMITS_CONFIG,
  DEFAULT_POOL_CONFIG,
  DEFAULT_STREAM_CONFIG,
  DEFAULT_TIMEOUT_CONFIG,
} from './config/validator.js';
export {
  classifyUpstreamError,
  isRetryableMethod,
  ProxyError,
  sendProxyError,
  validateIncomingRequest,
} from './http/errors.js';
export type { ProxyErrorCode } from './http/errors.js';
export {
  buildClientResponseHeaders,
  buildUpstreamRequestHeaders,
  parseConnectionHeaderTokens,
} from './http/headers.js';
export { HealthChecker } from './pool/health-checker.js';
export { LoadBalancer } from './pool/load-balancer.js';
export { UpstreamPool } from './pool/upstream-pool.js';
export { forwardRequest } from './proxy/forwarder.js';
export type { BackpressureMetrics, ForwarderDependencies } from './proxy/forwarder.js';
export { ProxyServer } from './proxy/proxy-server.js';
export type {
  HealthCheckConfig,
  LimitsConfig,
  LoadBalancingStrategy,
  PartialProxyConfig,
  PoolConfig,
  ProxyConfig,
  StreamConfig,
  TimeoutConfig,
  UpstreamConfig,
} from './types/config.js';
export type { RequestContext } from './types/context.js';
export type { UpstreamSnapshot, UpstreamTarget } from './types/upstream.js';
