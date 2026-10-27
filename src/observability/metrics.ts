import { monitorEventLoopDelay, performance } from 'node:perf_hooks';
import type { EventLoopUtilization, IntervalHistogram } from 'node:perf_hooks';
import type { ProxyErrorCode } from '../http/errors.js';
import type { UpstreamSnapshot } from '../types/upstream.js';
import { LatencyRingBuffer } from './ring-buffer.js';
import type { PercentileSummary } from './ring-buffer.js';

export interface EventLoopStats {
  delayMinMs: number;
  delayMeanMs: number;
  delayP50Ms: number;
  delayP95Ms: number;
  delayP99Ms: number;
  delayMaxMs: number;
  utilization: number;
}

export interface MemoryStats {
  rssBytes: number;
  heapTotalBytes: number;
  heapUsedBytes: number;
  externalBytes: number;
  arrayBuffersBytes: number;
}

export interface ProxyMetricsSnapshot {
  uptimeMs: number;
  requests: {
    total: number;
    active: number;
    retries: number;
    byStatusClass: {
      '2xx': number;
      '3xx': number;
      '4xx': number;
      '5xx': number;
    };
    errorsByCode: Record<string, number>;
  };
  latencyMs: {
    requestDuration: PercentileSummary;
    upstreamResponse: PercentileSummary;
  };
  backpressure: {
    clientPauseCount: number;
    upstreamPauseCount: number;
  };
  eventLoop: EventLoopStats;
  memory: MemoryStats;
  upstreams: UpstreamSnapshot[];
}

export class MetricsCollector {
  private readonly startedAtMs: number;
  private readonly loopHistogram: IntervalHistogram;
  private baselineElu: EventLoopUtilization;
  private readonly requestDurationRing = new LatencyRingBuffer(1024);
  private readonly upstreamLatencyRing = new LatencyRingBuffer(1024);

  private totalRequests = 0;
  private totalRetries = 0;
  private status2xx = 0;
  private status3xx = 0;
  private status4xx = 0;
  private status5xx = 0;
  private readonly errorsByCode = new Map<ProxyErrorCode, number>();

  constructor(resolutionMs = 10) {
    this.startedAtMs = performance.now();
    this.loopHistogram = monitorEventLoopDelay({ resolution: resolutionMs });
    this.loopHistogram.enable();
    this.baselineElu = performance.eventLoopUtilization();
  }

  recordRequestComplete(params: {
    statusCode: number;
    durationMs: number;
    upstreamLatencyMs?: number;
    retries: number;
    errorCode?: ProxyErrorCode;
  }): void {
    this.totalRequests += 1;
    if (params.retries > 0) {
      this.totalRetries += params.retries;
    }

    const code = params.statusCode;
    if (code >= 200 && code < 300) {
      this.status2xx += 1;
    } else if (code >= 300 && code < 400) {
      this.status3xx += 1;
    } else if (code >= 400 && code < 500) {
      this.status4xx += 1;
    } else if (code >= 500) {
      this.status5xx += 1;
    }

    if (params.errorCode !== undefined) {
      const prev = this.errorsByCode.get(params.errorCode) ?? 0;
      this.errorsByCode.set(params.errorCode, prev + 1);
    }

    this.requestDurationRing.record(params.durationMs);
    if (params.upstreamLatencyMs !== undefined) {
      this.upstreamLatencyRing.record(params.upstreamLatencyMs);
    }
  }

  snapshot(params: {
    activeRequests: number;
    clientPauseCount: number;
    upstreamPauseCount: number;
    upstreams: UpstreamSnapshot[];
  }): ProxyMetricsSnapshot {
    const elu = performance.eventLoopUtilization(this.baselineElu);
    const mem = process.memoryUsage();
    const errorCounts: Record<string, number> = {};
    for (const [k, v] of this.errorsByCode.entries()) {
      errorCounts[k] = v;
    }

    return {
      uptimeMs: round2(performance.now() - this.startedAtMs),
      requests: {
        total: this.totalRequests,
        active: params.activeRequests,
        retries: this.totalRetries,
        byStatusClass: {
          '2xx': this.status2xx,
          '3xx': this.status3xx,
          '4xx': this.status4xx,
          '5xx': this.status5xx,
        },
        errorsByCode: errorCounts,
      },
      latencyMs: {
        requestDuration: this.requestDurationRing.summarize(),
        upstreamResponse: this.upstreamLatencyRing.summarize(),
      },
      backpressure: {
        clientPauseCount: params.clientPauseCount,
        upstreamPauseCount: params.upstreamPauseCount,
      },
      eventLoop: {
        delayMinMs: nsToMs(this.loopHistogram.min),
        delayMeanMs: nsToMs(this.loopHistogram.mean),
        delayP50Ms: nsToMs(this.loopHistogram.percentile(50)),
        delayP95Ms: nsToMs(this.loopHistogram.percentile(95)),
        delayP99Ms: nsToMs(this.loopHistogram.percentile(99)),
        delayMaxMs: nsToMs(this.loopHistogram.max),
        utilization: round4(elu.utilization),
      },
      memory: {
        rssBytes: mem.rss,
        heapTotalBytes: mem.heapTotal,
        heapUsedBytes: mem.heapUsed,
        externalBytes: mem.external,
        arrayBuffersBytes: mem.arrayBuffers,
      },
      upstreams: params.upstreams,
    };
  }

  resetEventLoopStats(): void {
    this.loopHistogram.reset();
    this.baselineElu = performance.eventLoopUtilization();
  }

  stop(): void {
    this.loopHistogram.disable();
  }
}

function nsToMs(ns: number): number {
  if (!Number.isFinite(ns) || ns < 0 || ns > 1e15) {
    return 0;
  }
  return round2(ns / 1e6);
}

function round2(n: number): number {
  return Math.round(n * 100) / 100;
}

function round4(n: number): number {
  return Math.round(n * 10000) / 10000;
}
