import { describe, expect, it } from 'vitest';
import { RouteTable } from '../../src/gate/router/routeTable';

describe('RouteTable', () => {
  const table = new RouteTable([
    { prefix: 'game.', service: 'game' },
    { prefix: 'game.pvp.', service: 'battle' },
    { prefix: 'chat.', service: 'chat' },
    { cmd: 'game.pvp.surrender', service: 'game' },
    { prefix: 'rank.', service: 'rank', sticky: false, timeoutMs: 2000 },
  ]);

  it('routes by prefix', () => {
    expect(table.resolve('game.move')?.service).toBe('game');
    expect(table.resolve('chat.send')?.service).toBe('chat');
  });

  it('prefers the longest prefix', () => {
    expect(table.resolve('game.pvp.attack')?.service).toBe('battle');
  });

  it('prefers an exact match over any prefix', () => {
    expect(table.resolve('game.pvp.surrender')?.service).toBe('game');
  });

  it('carries per-route options', () => {
    const rank = table.resolve('rank.top');
    expect(rank).toMatchObject({ service: 'rank', sticky: false, timeoutMs: 2000 });
    expect(table.resolve('game.move')?.sticky).toBe(true);
  });

  it('returns null for an unroutable command when there is no default', () => {
    expect(table.resolve('mail.list')).toBeNull();
  });

  it('falls back to the default service when one is configured', () => {
    const withDefault = new RouteTable([{ prefix: 'chat.', service: 'chat' }], 'game');
    expect(withDefault.resolve('anything.at.all')?.service).toBe('game');
  });

  it('recognises gate-internal commands', () => {
    expect(table.isInternal('gate.whoami')).toBe(true);
    expect(table.isInternal('game.move')).toBe(false);
  });

  it('lists every reachable service', () => {
    expect(new Set(table.services())).toEqual(new Set(['game', 'battle', 'chat', 'rank']));
  });

  it('rejects a malformed rule at construction time', () => {
    expect(() => new RouteTable([{ service: 'game' }])).toThrow(/needs "cmd" or "prefix"/);
    expect(() => new RouteTable([{ prefix: 'x.' } as never])).toThrow(/missing "service"/);
  });
});
