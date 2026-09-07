/**
 * Guards the layering that src/framework, src/gate and src/client exist to
 * express:
 *
 *   framework/   base layer - knows nothing about who builds on it
 *   gate/        one service built on the framework
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
      violations('framework', (t) => !t.startsWith('gate') && !t.startsWith('client')),
    ).toEqual([]);
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
    expect(tsFiles(join(SRC, 'client')).length).toBeGreaterThan(0);
    expect(localImports(join(SRC, 'gate', 'gate.ts'))).toContain('framework/protocol/packet');
    expect(localImports(join(SRC, 'client', 'gateClient.ts'))).toContain(
      'framework/protocol/packet',
    );
  });
});
