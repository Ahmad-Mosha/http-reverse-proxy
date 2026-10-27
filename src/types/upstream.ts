import type { Agent } from 'node:http';

export interface UpstreamTarget {
  id: string;
  host: string;
  port: number;
  protocol: 'http:';
  healthCheckPath: string;
  healthy: boolean;
  activeConnections: number;
  consecutiveFailures: number;
  consecutiveSuccesses: number;
  totalRequests: number;
  totalErrors: number;
  agent: Agent;
}

export interface UpstreamSnapshot {
  id: string;
  host: string;
  port: number;
  healthy: boolean;
  activeConnections: number;
  consecutiveFailures: number;
  totalRequests: number;
  totalErrors: number;
}
