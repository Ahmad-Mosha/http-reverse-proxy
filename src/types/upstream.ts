import type { Agent } from 'node:http';

export interface UpstreamTarget {
  readonly id: string;
  readonly host: string;
  readonly port: number;
  readonly protocol: 'http:';
  healthy: boolean;
  activeConnections: number;
  totalRequests: number;
  totalErrors: number;
  readonly agent: Agent;
}

export interface UpstreamSnapshot {
  readonly id: string;
  readonly host: string;
  readonly port: number;
  readonly healthy: boolean;
  readonly activeConnections: number;
  readonly totalRequests: number;
  readonly totalErrors: number;
}
