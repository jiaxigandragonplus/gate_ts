import { describe, expect, it } from 'vitest';
import { GameSystem, SystemRegistry, type SystemHandler } from '../../src/game/system';

class Bag extends GameSystem<{ slots: number[] }> {
  readonly name = 'bag';
  override createState(): { slots: number[] } {
    return { slots: [] };
  }
  override handlers(): Record<string, SystemHandler<{ slots: number[] }>> {
    return { list: () => 'listed', use: () => 'used' };
  }
  publicApi(): string {
    return 'bag api';
  }
}

class Quest extends GameSystem<{ ids: number[] }> {
  readonly name = 'quest';
  override createState(): { ids: number[] } {
    return { ids: [] };
  }
  override handlers(): Record<string, SystemHandler<{ ids: number[] }>> {
    return { accept: () => 'accepted' };
  }
}

/** A system with no client-facing actions is legal: it can be event-only. */
class Analytics extends GameSystem<Record<string, never>> {
  readonly name = 'analytics';
  override createState(): Record<string, never> {
    return {};
  }
}

describe('SystemRegistry', () => {
  const build = (): SystemRegistry => {
    const r = new SystemRegistry();
    r.add(new Bag());
    r.add(new Quest());
    r.add(new Analytics());
    return r;
  };

  it('routes <system>.<action> to the right handler', () => {
    const r = build();
    expect(r.route('bag.list')?.system.name).toBe('bag');
    expect(r.route('quest.accept')?.system.name).toBe('quest');
  });

  it('returns nothing for an unknown system or action', () => {
    const r = build();
    expect(r.route('bag.nope')).toBeUndefined();
    expect(r.route('mail.list')).toBeUndefined();
    expect(r.route('bag')).toBeUndefined();
  });

  it('gives typed access to a sibling system', () => {
    const r = build();
    expect(r.get<Bag>('bag').publicApi()).toBe('bag api');
    expect(r.has('quest')).toBe(true);
    expect(r.has('mail')).toBe(false);
  });

  it('fails loudly on a mistyped dependency, listing what exists', () => {
    const r = build();
    // A silent undefined here would surface much later as a null deref.
    expect(() => r.get('bagg')).toThrow(/not registered.*bag, quest, analytics/s);
  });

  it('rejects a duplicate system name', () => {
    const r = build();
    expect(() => r.add(new Bag())).toThrow(/registered twice/);
  });

  it('rejects names that would break command routing', () => {
    // A dot in a system or action name would make the <system>.<action> split
    // ambiguous.
    class BadSystem extends GameSystem<null> {
      readonly name = 'bad.name';
      override createState(): null {
        return null;
      }
    }
    class BadAction extends GameSystem<null> {
      readonly name = 'ok';
      override createState(): null {
        return null;
      }
      override handlers(): Record<string, SystemHandler<null>> {
        return { 'bad.action': () => null };
      }
    }
    expect(() => new SystemRegistry().add(new BadSystem())).toThrow(/no dots or whitespace/);
    expect(() => new SystemRegistry().add(new BadAction())).toThrow(/no dots or whitespace/);
  });

  it('accepts a system with no client-facing actions', () => {
    const r = build();
    expect(r.get<Analytics>('analytics').name).toBe('analytics');
    expect(r.describe()).toContainEqual({ system: 'analytics', actions: [] });
  });

  it('describes itself for ops output', () => {
    expect(build().describe()).toEqual([
      { system: 'bag', actions: ['list', 'use'] },
      { system: 'quest', actions: ['accept'] },
      { system: 'analytics', actions: [] },
    ]);
  });
});
