/** One routing entry. Either `cmd` (exact) or `prefix` must be set. */
export interface RouteRule {
  /** Exact command match, e.g. "game.enter". Takes precedence over prefixes. */
  cmd?: string;
  /** Command prefix, e.g. "game.". Longest match wins. */
  prefix?: string;
  /** Target service name; must match the service a backend registers under. */
  service: string;
  /** Per-route override of the upstream request timeout (ms). */
  timeoutMs?: number;
  /**
   * When true (default) a uid is pinned to one backend node for the lifetime
   * of the binding, so stateful services keep their in-memory player state.
   * Set false for stateless services to spread load per request.
   */
  sticky?: boolean;
  /** Allow this command before authentication. Off by default. */
  allowAnonymous?: boolean;
}

export interface ResolvedRoute {
  service: string;
  cmd: string;
  timeoutMs?: number;
  sticky: boolean;
  allowAnonymous: boolean;
}

/** Commands under this prefix are answered by the gate itself. */
export const INTERNAL_PREFIX = 'gate.';

/**
 * Static command -> service map. Cheap enough to evaluate per packet: exact
 * matches are a hash lookup, prefixes are a short pre-sorted scan.
 */
export class RouteTable {
  private readonly exact = new Map<string, RouteRule>();
  private readonly prefixes: RouteRule[];

  constructor(
    rules: RouteRule[],
    private readonly defaultService?: string,
  ) {
    const prefixes: RouteRule[] = [];
    for (const rule of rules) {
      if (!rule.service) throw new Error(`route rule missing "service": ${JSON.stringify(rule)}`);
      if (rule.cmd) this.exact.set(rule.cmd, rule);
      else if (rule.prefix) prefixes.push(rule);
      else throw new Error(`route rule needs "cmd" or "prefix": ${JSON.stringify(rule)}`);
    }
    // Longest prefix first so "game.pvp." beats "game.".
    this.prefixes = prefixes.sort((a, b) => (b.prefix ?? '').length - (a.prefix ?? '').length);
  }

  isInternal(cmd: string): boolean {
    return cmd.startsWith(INTERNAL_PREFIX);
  }

  resolve(cmd: string): ResolvedRoute | null {
    const rule = this.match(cmd);
    if (!rule) {
      if (!this.defaultService) return null;
      return { service: this.defaultService, cmd, sticky: true, allowAnonymous: false };
    }
    return {
      service: rule.service,
      cmd,
      ...(rule.timeoutMs === undefined ? {} : { timeoutMs: rule.timeoutMs }),
      sticky: rule.sticky !== false,
      allowAnonymous: rule.allowAnonymous === true,
    };
  }

  private match(cmd: string): RouteRule | undefined {
    const hit = this.exact.get(cmd);
    if (hit) return hit;
    for (const rule of this.prefixes) {
      if (cmd.startsWith(rule.prefix as string)) return rule;
    }
    return undefined;
  }

  /** Every service this table can route to; used to warm up registry lookups. */
  services(): string[] {
    const set = new Set<string>();
    for (const rule of this.exact.values()) set.add(rule.service);
    for (const rule of this.prefixes) set.add(rule.service);
    if (this.defaultService) set.add(this.defaultService);
    return [...set];
  }

  describe(): Array<{ match: string; service: string }> {
    const out: Array<{ match: string; service: string }> = [];
    for (const [cmd, rule] of this.exact) out.push({ match: cmd, service: rule.service });
    for (const rule of this.prefixes) out.push({ match: `${rule.prefix}*`, service: rule.service });
    if (this.defaultService) out.push({ match: '*', service: this.defaultService });
    return out;
  }
}
