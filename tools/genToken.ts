/**
 * Issues a test JWT, standing in for the login service.
 *
 *   npm run token -- --uid player-1 [--ttl 3600] [--secret dev-secret]
 */
import jwt from 'jsonwebtoken';
import * as dotenv from 'dotenv';

dotenv.config();

function arg(name: string, fallback?: string): string | undefined {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? process.argv[i + 1] : fallback;
}

const uid = arg('uid', 'player-1') as string;
const ttl = Number.parseInt(arg('ttl', '3600') as string, 10);
const secret = arg('secret', process.env.JWT_SECRET ?? 'dev-secret-change-me') as string;
const issuer = arg('issuer', process.env.JWT_ISSUER);
const audience = arg('audience', process.env.JWT_AUDIENCE);

const token = jwt.sign(
  { sub: uid, name: `player_${uid}` },
  secret,
  {
    algorithm: 'HS256',
    expiresIn: ttl,
    ...(issuer ? { issuer } : {}),
    ...(audience ? { audience } : {}),
  },
);

process.stdout.write(`${token}\n`);
