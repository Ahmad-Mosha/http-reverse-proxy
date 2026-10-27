export interface PercentileSummary {
  count: number;
  mean: number;
  p50: number;
  p95: number;
  p99: number;
  max: number;
}

export class LatencyRingBuffer {
  private readonly buffer: Float64Array;
  private readonly capacity: number;
  private writeIndex = 0;
  private filled = 0;
  private totalCount = 0;

  constructor(capacity = 1024) {
    this.capacity = capacity;
    this.buffer = new Float64Array(capacity);
  }

  record(valueMs: number): void {
    this.buffer[this.writeIndex] = valueMs;
    this.writeIndex = (this.writeIndex + 1) % this.capacity;
    if (this.filled < this.capacity) {
      this.filled += 1;
    }
    this.totalCount += 1;
  }

  summarize(): PercentileSummary {
    if (this.filled === 0) {
      return { count: 0, mean: 0, p50: 0, p95: 0, p99: 0, max: 0 };
    }

    const sample = this.buffer.subarray(0, this.filled).slice().sort();
    let sum = 0;
    for (const val of sample) {
      sum += val;
    }

    const pick = (quantile: number): number => {
      const idx = Math.min(
        sample.length - 1,
        Math.max(0, Math.ceil(quantile * sample.length) - 1),
      );
      return round2(sample[idx] ?? 0);
    };

    return {
      count: this.totalCount,
      mean: round2(sum / sample.length),
      p50: pick(0.5),
      p95: pick(0.95),
      p99: pick(0.99),
      max: round2(sample[sample.length - 1] ?? 0),
    };
  }
}

function round2(n: number): number {
  return Math.round(n * 100) / 100;
}
