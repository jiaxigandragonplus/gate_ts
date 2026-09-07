/**
 * Compiles proto/gate.proto into a committed JSON descriptor module.
 *
 *   npm run proto:gen
 *
 * The descriptor is committed so the runtime never reads a .proto from disk -
 * no path juggling between `src/`, `dist/` and the docker image. A unit test
 * re-parses the .proto and fails if the committed descriptor has drifted.
 */
import { writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import protobuf from 'protobufjs';

const PROTO = resolve(__dirname, '../proto/gate.proto');
const OUT = resolve(__dirname, '../src/protocol/pb/descriptor.ts');

const root = protobuf.loadSync(PROTO);
const descriptor = root.toJSON();

const banner = `/**
 * GENERATED FILE - do not edit.
 *
 * Source: proto/gate.proto
 * Regenerate: npm run proto:gen
 */`;

writeFileSync(
  OUT,
  `${banner}\nexport const gateDescriptor = ${JSON.stringify(descriptor, null, 2)} as const;\n`,
  'utf8',
);

process.stdout.write(`wrote ${OUT}\n`);
