/**
 * Guards the layering that src/framework, src/gate, src/game and src/client
 * exist to express:
 *
 *   framework/   base layer - knows nothing about who builds on it
 *   gate/        the edge service, built on the framework
 *   game/        the business-logic service, built on the framework
 *   client/      the client SDK; must stay usable outside node
 *
 * The dangerous direction is inward-to-outward (framework reaching into gate)
 * and sideways (client pulling in ioredis through the framework's cluster
 * code, which would make the SDK unusable in a browser). Both are checked
 * here rather than left to review.
 */
import { describe, expect, it } from 'vitest';
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join, resolve, dirname, relative } from 'node:path';

const SRC = resolve(__dirname, '../../src');

function tsFiles(dir: string): string[] {
  const out: string[] = [];
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) out.push(...tsFiles(full));
    else if (entry.endsWith('.ts')) out.push(full);
  }
  return out;
}

const IMPORT_RE = /(?:^|\n)\s*(?:import|export)[\s\S]*?from\s+'(\.[^']+)'/g;

/** Every relative import in `file`, as a path relative to src/. */
function localImports(file: string): string[] {
  const source = readFileSync(file, 'utf8');
  const targets: string[] = [];
  for (const match of source.matchAll(IMPORT_RE)) {
    targets.push(relative(SRC, resolve(dirname(file), match[1] as string)));
  }
  return targets;
}

/** Edges from `dir` whose target is not allowed by `permitted`. */
function violations(dir: string, permitted: (target: string) => boolean): string[] {
  const out: string[] = [];
  for (const file of tsFiles(join(SRC, dir))) {
    for (const target of localImports(file)) {
      if (!permitted(target)) out.push(`${relative(SRC, file)} -> ${target}`);
    }
  }
  return out;
}

describe('module boundaries', () => {
  it('the framework does not depend on anything built on it', () => {
    // A backend service importing the framework must not drag in the gateway.
    expect(
      violations(
        'framework',
        (t) => !t.startsWith('gate') && !t.startsWith('client') && !t.startsWith('game'),
      ),
    ).toEqual([]);
  });

  it('the services do not depend on each other', () => {
    // gate and game talk over the wire, never by importing each other: that
    // is what lets them scale and deploy independently.
    expect(violations('gate', (t) => !t.startsWith('game'))).toEqual([]);
    expect(violations('game', (t) => !t.startsWith('gate') && !t.startsWith('client'))).toEqual(
      [],
    );
  });

  it('the game node builds on the framework, not on its own copy of it', () => {
    // Every non-local import in game/ must resolve into the framework.
    const outside = new Set<string>();
    for (const file of tsFiles(join(SRC, 'game'))) {
      for (const target of localImports(file)) {
        if (!target.startsWith('game/')) outside.add(target.split('/').slice(0, 2).join('/'));
      }
    }
    for (const target of outside) {
      expect(target.startsWith('framework/')).toBe(true);
    }
    expect(outside.size).toBeGreaterThan(0);
  });

  it('the client SDK depends on the protocol only', () => {
    // Anything else (redis, the service node) makes the SDK node-only and
    // unusable from a browser or game engine.
    expect(violations('client', (t) => t.startsWith('framework/protocol/'))).toEqual([]);
  });

  it('the gate is not a backend service and not a client', () => {
    expect(
      violations(
        'gate',
        (t) => t !== 'framework/serviceNode' && !t.startsWith('client'),
      ),
    ).toEqual([]);
  });

  it('the protocol stays a leaf', () => {
    // The wire contract must be safe to depend on from anywhere.
    expect(
      violations('framework/protocol', (t) => t.startsWith('framework/protocol/')),
    ).toEqual([]);
  });

  it('finds the files it claims to check', () => {
    // A silent glob failure would make every assertion above vacuous.
    expect(tsFiles(join(SRC, 'gate')).length).toBeGreaterThan(10);
    expect(tsFiles(join(SRC, 'framework')).length).toBeGreaterThan(10);
    expect(tsFiles(join(SRC, 'game')).length).toBeGreaterThan(5);
    expect(tsFiles(join(SRC, 'client')).length).toBeGreaterThan(0);
    expect(localImports(join(SRC, 'gate', 'gate.ts'))).toContain('framework/protocol/packet');
    expect(localImports(join(SRC, 'client', 'gateClient.ts'))).toContain(
      'framework/protocol/packet',
    );
  });
});
