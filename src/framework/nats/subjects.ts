/**
 * Subject layout, the NATS counterpart of ../redis/keys.ts.
 *
 *   <p>.node.<gateId>            one gate's inbox (responses, pushes, kicks)
 *   <p>.broadcast                every gate
 *   <p>.svc.<service>.<nodeId>   one service node's inbox
 *   <p>.svc.<service>.q          queue-group subject for stateless work
 *
 * Kept hierarchical on purpose: `<p>.node.>` and `<p>.svc.game.>` are then
 * usable for monitoring and for NATS account permissions, which a flat
 * naming scheme would rule out.
 */

/**
 * NATS reserves these in subject tokens; whitespace is invalid too.
 *
 * Two constants on purpose: a /g/ regex keeps `lastIndex` between calls, so
 * reusing one for both `.replace` and `.test` makes `.test` alternate between
 * true and false for the same input.
 */
const ILLEGAL_TOKEN_CHARS_GLOBAL = /[\s.*>]+/g;
const HAS_ILLEGAL_TOKEN_CHAR = /[\s.*>]/;

export class InvalidSubjectTokenError extends Error {
  constructor(what: string, value: string) {
    super(
      `${what} "${value}" is not usable in a NATS subject: ` +
        'it must not contain "." "*" ">" or whitespace',
    );
    this.name = 'InvalidSubjectTokenError';
  }
}

/**
 * Make an id safe to use as one subject token.
 *
 * Call this once where the id is created (gate config, ServiceNode
 * constructor) so the same value is used for redis keys, subjects, logs and
 * metric labels. Two ids that differ only in illegal characters would
 * normalize to the same token and then share an inbox, so normalize at the
 * source rather than at every use.
 */
export function normalizeNodeId(raw: string): string {
  const cleaned = raw.replace(ILLEGAL_TOKEN_CHARS_GLOBAL, '_');
  // An id made only of illegal characters ("...") would normalize to "_":
  // a valid token, but almost certainly a misconfiguration, and every such id
  // would normalize to the same thing and share an inbox. Fail loudly instead.
  if (cleaned.length === 0 || /^_+$/.test(cleaned)) {
    throw new InvalidSubjectTokenError('node id', raw);
  }
  return cleaned;
}

/**
 * Reject an id that is not subject-safe.
 *
 * Node ids reach a gate through the node registry, i.e. they are written by
 * another process. A nodeId of `>` would otherwise turn a targeted publish
 * into a cluster-wide fan-out, so this is a guard against subject injection,
 * not just a typo check.
 */
export function assertSubjectToken(value: string, what = 'subject token'): string {
  if (value.length === 0 || HAS_ILLEGAL_TOKEN_CHAR.test(value)) {
    throw new InvalidSubjectTokenError(what, value);
  }
  return value;
}

export class Subjects {
  constructor(private readonly prefix: string) {
    assertSubjectToken(prefix, 'subject prefix');
  }

  /** Inbox of one gate. */
  node(gateId: string): string {
    return `${this.prefix}.node.${assertSubjectToken(gateId, 'gate id')}`;
  }

  /**
   * Every gate. Subscribed without a queue group so all gates get a copy.
   *
   * Deliberately outside the `node.` namespace: as `node.all` it would be the
   * inbox of a gate whose id happens to be "all", and messages targeted at
   * that gate would fan out to the whole cluster.
   */
  allNodes(): string {
    return `${this.prefix}.broadcast`;
  }

  /** Inbox of one service node. */
  serviceNode(service: string, nodeId: string): string {
    return (
      `${this.prefix}.svc.${assertSubjectToken(service, 'service name')}` +
      `.${assertSubjectToken(nodeId, 'service node id')}`
    );
  }

  /**
   * Shared subject for work that any node of a service may handle. Reserved
   * for stateless routes; the gate does not publish here yet (it pins a uid
   * to a node so stateful services keep their in-memory state).
   */
  serviceQueue(service: string): string {
    return `${this.prefix}.svc.${assertSubjectToken(service, 'service name')}.q`;
  }

  /** Wildcard covering everything this cluster uses; for monitoring. */
  all(): string {
    return `${this.prefix}.>`;
  }
}
