import { describe, expect, it } from 'vitest';
import jwt from 'jsonwebtoken';
import { generateKeyPairSync } from 'node:crypto';
import { JwtVerifier, AuthError } from '../../src/auth/jwt';
import type { JwtConfig } from '../../src/config';

const secret = 'unit-test-secret';
const base: JwtConfig = {
  secret,
  algorithms: ['HS256'],
  uidClaim: 'sub',
  clockToleranceSec: 0,
};

describe('JwtVerifier', () => {
  it('accepts a valid token and extracts the uid', () => {
    const token = jwt.sign({ sub: 'player-9' }, secret, { expiresIn: 60 });
    const user = new JwtVerifier(base).verify(token);
    expect(user.uid).toBe('player-9');
    expect(user.expiresAt).toBeGreaterThan(Date.now());
  });

  it('reads the uid from a custom claim', () => {
    const token = jwt.sign({ sub: 'ignored', playerId: 42 }, secret, { expiresIn: 60 });
    const user = new JwtVerifier({ ...base, uidClaim: 'playerId' }).verify(token);
    expect(user.uid).toBe('42');
  });

  it('rejects an expired token', () => {
    const token = jwt.sign({ sub: 'a' }, secret, { expiresIn: -10 });
    expect(() => new JwtVerifier(base).verify(token)).toThrow(
      expect.objectContaining({ reason: 'expired' }),
    );
  });

  it('rejects a token signed with the wrong key', () => {
    const token = jwt.sign({ sub: 'a' }, 'other-secret', { expiresIn: 60 });
    expect(() => new JwtVerifier(base).verify(token)).toThrow(AuthError);
  });

  it('rejects a token with no uid claim', () => {
    const token = jwt.sign({ role: 'admin' }, secret, { expiresIn: 60 });
    expect(() => new JwtVerifier(base).verify(token)).toThrow(
      expect.objectContaining({ reason: 'missing_uid' }),
    );
  });

  it('enforces issuer and audience when configured', () => {
    const cfg = { ...base, issuer: 'auth.example.com', audience: 'game' };
    const good = jwt.sign({ sub: 'a' }, secret, {
      expiresIn: 60,
      issuer: 'auth.example.com',
      audience: 'game',
    });
    expect(new JwtVerifier(cfg).verify(good).uid).toBe('a');

    const wrongIssuer = jwt.sign({ sub: 'a' }, secret, {
      expiresIn: 60,
      issuer: 'evil.example.com',
      audience: 'game',
    });
    expect(() => new JwtVerifier(cfg).verify(wrongIssuer)).toThrow(AuthError);
  });

  it('refuses an algorithm the config does not allow', () => {
    // "alg: none" and algorithm confusion are the classic JWT attacks.
    const token = jwt.sign({ sub: 'a' }, secret, { algorithm: 'HS384', expiresIn: 60 });
    expect(() => new JwtVerifier(base).verify(token)).toThrow(AuthError);
  });

  it('verifies RS256 tokens against a public key', () => {
    const { privateKey, publicKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
    const pem = publicKey.export({ type: 'spki', format: 'pem' }).toString();
    const token = jwt.sign({ sub: 'rsa-player' }, privateKey.export({ type: 'pkcs8', format: 'pem' }).toString(), {
      algorithm: 'RS256',
      expiresIn: 60,
    });
    const verifier = new JwtVerifier({
      publicKey: pem,
      algorithms: ['RS256'],
      uidClaim: 'sub',
      clockToleranceSec: 0,
    });
    expect(verifier.verify(token).uid).toBe('rsa-player');
  });

  it('requires a key at construction time', () => {
    expect(() => new JwtVerifier({ algorithms: ['HS256'], uidClaim: 'sub', clockToleranceSec: 0 })).toThrow(
      /requires either a secret or a public key/,
    );
  });
});
