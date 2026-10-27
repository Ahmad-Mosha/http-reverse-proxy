export type LoadBalancingStrategy = 'round-robin' | 'least-connections';

export interface UpstreamConfig {
  readonly id: string;
  readonly host: string;
  readonly port: number;
  readonly protocol?: 'http:';
}

export interface PoolConfig {
  /** Reuse TCP connections to upstream servers via HTTP/1.1 Keep-Alive. */
  readonly keepAlive: boolean;
  /** Initial delay (ms) for TCP Keep-Alive probes on idle upstream sockets. */
  readonly keepAliveMsecs: number;
  /** Maximum concurrent active sockets allowed per upstream host:port. */
  readonly maxSocketsPerUpstream: number;
  /** Maximum idle sockets retained in the free pool per upstream host:port. */
  readonly maxFreeSocketsPerUpstream: number;
  /**
   * Socket reuse scheduling policy:
   * - 'lifo' selects the most recently used idle socket (keeps hot sockets active, lets cold ones expire).
   * - 'fifo' selects the oldest idle socket.
   */
  readonly scheduling: 'lifo' | 'fifo';
}

export interface TimeoutConfig {
  /** Max time (ms) allowed for upstream to send response headers after request starts. */
  readonly upstreamResponseTimeoutMs: number;
  /** Max time (ms) allowed for an idle upstream socket before closing. */
  readonly upstreamSocketTimeoutMs: number;
  /** Max time (ms) allowed for a client to send full HTTP headers. */
  readonly clientHeadersTimeoutMs: number;
  /** Max time (ms) allowed for receiving the entire client request. */
  readonly clientRequestTimeoutMs: number;
  /** Time (ms) the proxy waits for additional client requests on a Keep-Alive socket. */
  readonly clientKeepAliveTimeoutMs: number;
}

export interface StreamConfig {
  /** Buffer threshold (in bytes) before stream backpressure signals pause(). */
  readonly highWaterMarkBytes: number;
}

export interface ProxyConfig {
  readonly host: string;
  readonly port: number;
  readonly strategy: LoadBalancingStrategy;
  readonly upstreams: readonly UpstreamConfig[];
  readonly pool: PoolConfig;
  readonly timeouts: TimeoutConfig;
  readonly stream: StreamConfig;
  /** Whether to append to incoming X-Forwarded-* headers or overwrite them. */
  readonly trustProxy: boolean;
}

export interface PartialProxyConfig {
  readonly host?: string;
  readonly port?: number;
  readonly strategy?: LoadBalancingStrategy;
  readonly upstreams: readonly UpstreamConfig[];
  readonly pool?: Partial<PoolConfig>;
  readonly timeouts?: Partial<TimeoutConfig>;
  readonly stream?: Partial<StreamConfig>;
  readonly trustProxy?: boolean;
}
