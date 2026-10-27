import { request as httpRequest } from 'node:http';
import type { HealthCheckConfig } from '../types/config.js';
import type { UpstreamTarget } from '../types/upstream.js';
import type { UpstreamPool } from './upstream-pool.js';

export class HealthChecker {
  private readonly pool: UpstreamPool;
  private readonly config: HealthCheckConfig;
  private timer: NodeJS.Timeout | null = null;
  private cycleInProgress = false;

  constructor(pool: UpstreamPool, config: HealthCheckConfig) {
    this.pool = pool;
    this.config = config;
  }

  start(): void {
    if (!this.config.enabled || this.timer !== null) {
      return;
    }

    this.timer = setInterval(() => {
      void this.runProbeCycle();
    }, this.config.intervalMs);
    this.timer.unref();
  }

  stop(): void {
    if (this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  async runProbeCycle(): Promise<void> {
    if (this.cycleInProgress) {
      return;
    }
    this.cycleInProgress = true;
    try {
      const targets = this.pool.getAllUpstreams();
      await Promise.all(
        targets.map(async (target) => {
          const ok = await this.probeUpstream(target);
          this.pool.recordProbeResult(target, ok);
        }),
      );
    } finally {
      this.cycleInProgress = false;
    }
  }

  private probeUpstream(target: UpstreamTarget): Promise<boolean> {
    return new Promise<boolean>((resolve) => {
      let settled = false;
      const done = (ok: boolean): void => {
        if (!settled) {
          settled = true;
          clearTimeout(timeoutId);
          resolve(ok);
        }
      };

      const req = httpRequest(
        {
          protocol: target.protocol,
          host: target.host,
          port: target.port,
          method: 'GET',
          path: target.healthCheckPath,
          headers: {
            host: `${target.host}:${target.port}`,
            'user-agent': 'relay-edge-healthcheck/1.0',
            connection: 'close',
          },
          agent: false,
        },
        (res) => {
          res.resume();
          const status = res.statusCode ?? 500;
          done(status >= 200 && status < 300);
        },
      );

      const timeoutId = setTimeout(() => {
        req.destroy();
        done(false);
      }, this.config.timeoutMs);
      timeoutId.unref();

      req.on('error', () => {
        done(false);
      });

      req.end();
    });
  }
}
