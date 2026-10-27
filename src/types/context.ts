import type { ProxyErrorCode } from '../http/errors.js';

export interface RequestContext {
  requestId: string;
  method: string;
  url: string;
  clientIp: string;
  startTimeMs: number;
  attempts: number;
  upstreamId?: string;
  statusCode?: number;
  upstreamLatencyMs?: number;
  errorCode?: ProxyErrorCode;
}
