import jwt, { type JwtPayload, type VerifyOptions, type Algorithm } from 'jsonwebtoken';
import type { JwtConfig } from '../config';

export interface AuthedUser {
  uid: string;
  /** JWT id, used for replay detection / revocation lists. */
  jti?: string;
  /** Expiry in ms since epoch, if the token carries one. */
  expiresAt?: number;
  claims: JwtPayload;
}

export class AuthError extends Error {
  constructor(
    message: string,
    readonly reason:
      | 'malformed'
      | 'expired'
      | 'signature'
      | 'claims'
      | 'missing_uid'
      | 'revoked',
  ) {
    super(message);
    this.name = 'AuthError';
  }
}

/**
 * Verifies JWTs minted by the login/auth service. The gate never issues
 * tokens - it only validates them.
 */
export class JwtVerifier {
  private readonly key: string;
  private readonly options: VerifyOptions;

  constructor(private readonly cfg: JwtConfig) {
    const key = cfg.publicKey ?? cfg.secret;
    if (!key) throw new Error('JwtVerifier requires either a secret or a public key');
    this.key = key;
    this.options = {
      algorithms: cfg.algorithms as Algorithm[],
      clockTolerance: cfg.clockToleranceSec,
      ...(cfg.issuer ? { issuer: cfg.issuer } : {}),
      ...(cfg.audience ? { audience: cfg.audience } : {}),
    };
  }

  verify(token: string): AuthedUser {
    let payload: JwtPayload;
    try {
      const decoded = jwt.verify(token, this.key, this.options);
      if (typeof decoded === 'string') throw new AuthError('token payload is not an object', 'malformed');
      payload = decoded;
    } catch (err) {
      throw toAuthError(err);
    }

    const rawUid = payload[this.cfg.uidClaim];
    const uid =
      typeof rawUid === 'string' ? rawUid : typeof rawUid === 'number' ? String(rawUid) : '';
    if (!uid) {
      throw new AuthError(`token is missing the "${this.cfg.uidClaim}" claim`, 'missing_uid');
    }

    return {
      uid,
      ...(typeof payload.jti === 'string' ? { jti: payload.jti } : {}),
      ...(typeof payload.exp === 'number' ? { expiresAt: payload.exp * 1000 } : {}),
      claims: payload,
    };
  }
}

function toAuthError(err: unknown): AuthError {
  if (err instanceof AuthError) return err;
  if (err instanceof jwt.TokenExpiredError) return new AuthError('token expired', 'expired');
  if (err instanceof jwt.NotBeforeError) return new AuthError('token not yet valid', 'claims');
  if (err instanceof jwt.JsonWebTokenError) {
    const reason = err.message.includes('signature') ? 'signature' : 'claims';
    return new AuthError(err.message, reason);
  }
  return new AuthError('token verification failed', 'malformed');
}
