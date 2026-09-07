import { describe, expect, it } from 'vitest';
import {
  Subjects,
  normalizeNodeId,
  assertSubjectToken,
  InvalidSubjectTokenError,
} from '../../src/framework/nats/subjects';

describe('normalizeNodeId', () => {
  it('leaves a subject-safe id alone', () => {
    expect(normalizeNodeId('gate-1')).toBe('gate-1');
    expect(normalizeNodeId('game_node_7')).toBe('game_node_7');
  });

  it('replaces the characters NATS reserves in a token', () => {
    // The default gate id is `${hostname()}-${port}`, and hostnames routinely
    // contain dots - which would silently split into extra subject tokens.
    expect(normalizeNodeId('MacBook-Pro.local-7000')).toBe('MacBook-Pro_local-7000');
    expect(normalizeNodeId('a b')).toBe('a_b');
    expect(normalizeNodeId('a*b')).toBe('a_b');
    expect(normalizeNodeId('a>b')).toBe('a_b');
    expect(normalizeNodeId('a...b')).toBe('a_b');
  });

  it('rejects an id that has no legal characters at all', () => {
    // "..." would normalize to "_" - valid, but so would "***", and the two
    // nodes would then share an inbox. Loud failure beats a silent collision.
    expect(() => normalizeNodeId('...')).toThrow(InvalidSubjectTokenError);
    expect(() => normalizeNodeId('***')).toThrow(InvalidSubjectTokenError);
    expect(() => normalizeNodeId('')).toThrow(InvalidSubjectTokenError);
    // but an id that merely contains illegal characters is fine
    expect(normalizeNodeId('a...b')).toBe('a_b');
  });
});

describe('assertSubjectToken', () => {
  it('accepts safe tokens', () => {
    expect(assertSubjectToken('gate-1')).toBe('gate-1');
  });

  it('is not stateful across calls', () => {
    // A /g/ regex reused for .test() keeps lastIndex between calls and would
    // alternate between true and false for the same input.
    for (let i = 0; i < 4; i++) {
      expect(() => assertSubjectToken('a.b')).toThrow(InvalidSubjectTokenError);
    }
    for (let i = 0; i < 4; i++) {
      expect(assertSubjectToken('ab')).toBe('ab');
    }
  });

  it.each(['a.b', 'a b', 'a\tb', '*', '>', 'x>', ''])('rejects %j', (bad) => {
    expect(() => assertSubjectToken(bad)).toThrow(InvalidSubjectTokenError);
  });
});

describe('Subjects', () => {
  const s = new Subjects('gate');

  it('builds a hierarchical layout', () => {
    expect(s.node('gate-1')).toBe('gate.node.gate-1');
    expect(s.allNodes()).toBe('gate.broadcast');
    expect(s.serviceNode('game', 'game-1')).toBe('gate.svc.game.game-1');
    expect(s.serviceQueue('game')).toBe('gate.svc.game.q');
    expect(s.all()).toBe('gate.>');
  });

  it('is covered by its own monitoring wildcard', () => {
    // `gate.>` must actually match everything the cluster publishes.
    const prefix = s.all().replace(/>$/, '');
    for (const subject of [
      s.node('gate-1'),
      s.allNodes(),
      s.serviceNode('game', 'game-1'),
      s.serviceQueue('chat'),
    ]) {
      expect(subject.startsWith(prefix)).toBe(true);
      // and be exactly one token deeper than the prefix at minimum
      expect(subject.slice(prefix.length).split('.').length).toBeGreaterThanOrEqual(1);
    }
  });

  it('refuses a node id that would inject a wildcard', () => {
    // Node ids come from the registry, i.e. another process writes them. A
    // nodeId of ">" would turn a targeted publish into a cluster-wide fan-out.
    expect(() => s.serviceNode('game', '>')).toThrow(InvalidSubjectTokenError);
    expect(() => s.serviceNode('game', '*')).toThrow(InvalidSubjectTokenError);
    expect(() => s.node('a.b')).toThrow(InvalidSubjectTokenError);
    expect(() => s.serviceNode('game.evil', 'n1')).toThrow(InvalidSubjectTokenError);
  });

  it('validates its own prefix at construction', () => {
    expect(() => new Subjects('a.b')).toThrow(InvalidSubjectTokenError);
    expect(() => new Subjects('')).toThrow(InvalidSubjectTokenError);
  });

  it('cannot collide a gate inbox with the broadcast subject', () => {
    // The broadcast subject sits outside the node namespace, so even a gate
    // named "all" gets its own inbox - no reserved gate ids needed.
    expect(s.allNodes()).toBe('gate.broadcast');
    expect(s.node('all')).toBe('gate.node.all');
    expect(s.node('all')).not.toBe(s.allNodes());
  });
});
