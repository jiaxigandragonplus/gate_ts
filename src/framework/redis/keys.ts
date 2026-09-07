/**
 * Central key layout. Everything the gate cluster stores lives under one
 * prefix so a single `SCAN`/`DEL` can wipe an environment.
 *
 *   <p>:sess:<uid>            string  JSON SessionOwner   who owns this account right now
 *   <p>:sid:<sid>             string  uid                 reverse lookup for resume
 *   <p>:resume:<sid>          string  JSON ResumeTicket    resume credentials, TTL = resume window
 *   <p>:nodes                 hash    gateId -> JSON NodeInfo
 *   <p>:svc:<service>:nodes   hash    nodeId -> JSON NodeInfo
 *   <p>:bind:<uid>:<service>  string  nodeId              sticky backend pinning
 *   <p>:node:<gateId>         channel inbound messages for one gate
 *   <p>:broadcast             channel inbound messages for every gate
 *   <p>:svc:<service>:<node>  channel inbound messages for one service node
 */
export class Keys {
  constructor(private readonly p: string) {}

  session(uid: string): string {
    return `${this.p}:sess:${uid}`;
  }

  sidIndex(sid: string): string {
    return `${this.p}:sid:${sid}`;
  }

  resumeTicket(sid: string): string {
    return `${this.p}:resume:${sid}`;
  }

  gateNodes(): string {
    return `${this.p}:nodes`;
  }

  serviceNodes(service: string): string {
    return `${this.p}:svc:${service}:nodes`;
  }

  backendBinding(uid: string, service: string): string {
    return `${this.p}:bind:${uid}:${service}`;
  }

  nodeChannel(gateId: string): string {
    return `${this.p}:node:${gateId}`;
  }

  /**
   * Outside the `node:` namespace on purpose - as `node:all` it would be the
   * inbox of a gate whose id is "all", whose targeted messages would then
   * reach every gate.
   */
  allNodesChannel(): string {
    return `${this.p}:broadcast`;
  }

  serviceChannel(service: string, nodeId: string): string {
    return `${this.p}:svc:${service}:${nodeId}`;
  }

  onlineCounter(): string {
    return `${this.p}:stats:online`;
  }
}
