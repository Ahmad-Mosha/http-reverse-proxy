export interface RequestContext {
  readonly requestId: string;
  readonly method: string;
  readonly url: string;
  readonly clientIp: string;
  readonly startTimeMs: number;
  upstreamId?: string;
  statusCode?: number;
}
