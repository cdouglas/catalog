#!/usr/bin/env node
// Decodes catalog files with the format explorer's own decoder, headless, and
// reports what it could not decode: a decode error, a field catalog.proto
// lacks, a wire type its declaration does not allow, an unknown enum value, a
// structural payload of another layout version, or bytes no node covers.
//
// Usage (from fileio-catalog/):
//   node docs/format-explorer/check.mjs                 # the embedded scenarios
//   node docs/format-explorer/check.mjs <file|dir>...   # catalog files too
//
// To collect the catalog files a suite writes:
//   mvn test -Dtest=TestLocalCatalog -Dexplorer.dump=target/explorer-dump
//   node docs/format-explorer/check.mjs target/explorer-dump
//
// Exits 1 if any file has a problem.
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { gunzipSync } from 'node:zlib';
import vm from 'node:vm';

const html = readFileSync(join(dirname(fileURLToPath(import.meta.url)), 'index.html'), 'utf8');
const fixtures = JSON.parse(
  html.match(/<script id="fixtures-data"[^>]*>([\s\S]*?)<\/script>/)[1]);
const code = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)].map((m) => m[1]).join('\n');

const window = {};
vm.runInNewContext(code, {
  window,
  document: { addEventListener() {}, getElementById() { return null; } },
  BigInt, TextDecoder, Uint8Array, Number, String, JSON, Math, Error, Date, Promise,
});
const explorer = window.formatExplorer;
explorer.load(fixtures);

const INLINE_TM = {
  'CreateTableInline.metadata': 'metadata_codec',
  'UpdateTableInline.full_metadata': 'full_metadata_codec',
  'InlineTable.metadata': 'metadata_codec',
};

const seen = new Set();
let structural = 0;

function problems(bytes) {
  const found = [];
  let decoded;
  try {
    decoded = explorer.decode(bytes);
  } catch (e) {
    return [`decode error: ${e.message}`];
  }
  const covered = new Uint8Array(bytes.length);
  for (const node of decoded.nodes) {
    if (node.kind === 'msg' && node.messageType) seen.add(node.messageType);
    if (node.drift) found.push(`${node.schemaPath || node.label} @${node.byteStart}: ${node.drift}`);
    else if (node.quirk && node.quirkNote && node.quirkNote.startsWith('SCHEMA DRIFT')) {
      found.push(`${node.schemaPath || node.label} @${node.byteStart}: ${node.quirkNote}`);
    }
    if (!node.children || node.children.length === 0) {
      for (let i = node.byteStart; i < node.byteEnd; i++) covered[i] = 1;
    }
  }
  const uncovered = covered.indexOf(0);
  if (uncovered >= 0) found.push(`byte ${uncovered} is in no leaf node`);
  // Inline table metadata in the structural codec: the explorer's walker must
  // know the layout version.
  for (const node of decoded.nodes) {
    if (node.kind !== 'msg' || !node.children) continue;
    for (const [path, codecField] of Object.entries(INLINE_TM)) {
      const [message, field] = path.split('.');
      if (node.messageType !== message) continue;
      const data = node.children.find((c) => c.kind === 'bytes' && c.label === field);
      const codec = node.children.find((c) => c.kind === 'varint' && c.label === codecField);
      if (!data || !codec || !codec.decodedValue.startsWith('1')) continue;
      structural++;
      const view = explorer.structuralView(
        new Uint8Array(gunzipSync(bytes.slice(data.byteStart, data.byteEnd))));
      if (view.recoveredJson.startsWith('// Unsupported')) {
        found.push(`${path} @${data.byteStart}: ${view.recoveredJson.split('\n')[0]}`);
      }
    }
  }
  return found;
}

function* files(paths) {
  for (const path of paths) {
    if (statSync(path).isDirectory()) {
      yield* files(readdirSync(path).sort().map((name) => join(path, name)));
    } else {
      yield path;
    }
  }
}

let checked = 0;
let failed = 0;
function report(name, bytes) {
  checked++;
  const found = problems(bytes);
  if (found.length > 0) {
    failed++;
    if (failed <= 20) console.log(`${name}:\n  ${found.slice(0, 5).join('\n  ')}`);
  }
}

for (const s of fixtures.scenarios) {
  report(`scenario ${s.name}`, Uint8Array.from(Buffer.from(s.hex, 'hex')));
}
for (const path of files(process.argv.slice(2))) {
  report(path, new Uint8Array(readFileSync(path)));
}
const declared = Object.keys(fixtures.schema.messages);
const unseen = declared.filter((m) => !seen.has(m));
console.log(`${checked} files decoded, ${failed} with problems; ${structural} structural `
  + `payloads walked; ${declared.length - unseen.length} of ${declared.length} messages seen`
  + (unseen.length ? ` (not: ${unseen.join(', ')})` : ''));
process.exit(failed > 0 ? 1 : 0);
