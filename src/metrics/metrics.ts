/**
 * Minimal in-process counters rendered in Prometheus text format. Deliberately
 * dependency-free: a gate should not pull a metrics framework to expose a
 * couple of dozen numbers.
 */
export class Metrics {
  readonly startedAt = Date.now();

  authOk = 0;
  authFailed = 0;
  resumeOk = 0;
  resumeFailed = 0;
  resumeMigrated = 0;
  kicksDuplicateLogin = 0;
  kicksAdmin = 0;
  upstreamSent = 0;
  upstreamUnavailable = 0;
  upstreamTimeouts = 0;
  routeMisses = 0;
  downstreamResponses = 0;
  downstreamPushes = 0;
  downstreamDropped = 0;
  /** Histogram-free latency summary for upstream requests. */
  upstreamLatencySumMs = 0;
  upstreamLatencyCount = 0;

  private gauges: Record<string, () => number> = {};

  registerGauge(name: string, fn: () => number): void {
    this.gauges[name] = fn;
  }

  observeUpstreamLatency(ms: number): void {
    this.upstreamLatencySumMs += ms;
    this.upstreamLatencyCount += 1;
  }

  snapshot(): Record<string, number> {
    const out: Record<string, number> = {
      uptime_seconds: Math.floor((Date.now() - this.startedAt) / 1000),
      auth_ok_total: this.authOk,
      auth_failed_total: this.authFailed,
      resume_ok_total: this.resumeOk,
      resume_failed_total: this.resumeFailed,
      resume_migrated_total: this.resumeMigrated,
      kicks_duplicate_login_total: this.kicksDuplicateLogin,
      kicks_admin_total: this.kicksAdmin,
      upstream_sent_total: this.upstreamSent,
      upstream_unavailable_total: this.upstreamUnavailable,
      upstream_timeouts_total: this.upstreamTimeouts,
      route_misses_total: this.routeMisses,
      downstream_responses_total: this.downstreamResponses,
      downstream_pushes_total: this.downstreamPushes,
      downstream_dropped_total: this.downstreamDropped,
      upstream_latency_ms_sum: Math.round(this.upstreamLatencySumMs),
      upstream_latency_count: this.upstreamLatencyCount,
    };
    for (const [name, fn] of Object.entries(this.gauges)) out[name] = fn();
    return out;
  }

  render(labels: Record<string, string> = {}): string {
    const labelStr = Object.entries(labels)
      .map(([k, v]) => `${k}="${v.replace(/"/g, '')}"`)
      .join(',');
    const suffix = labelStr ? `{${labelStr}}` : '';
    const lines: string[] = [];
    for (const [name, value] of Object.entries(this.snapshot())) {
      const type = name.endsWith('_total') || name.endsWith('_sum') || name.endsWith('_count')
        ? 'counter'
        : 'gauge';
      lines.push(`# TYPE gate_${name} ${type}`);
      lines.push(`gate_${name}${suffix} ${value}`);
    }
    return `${lines.join('\n')}\n`;
  }
}
