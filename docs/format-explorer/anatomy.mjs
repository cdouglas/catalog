#!/usr/bin/env node
// Where a captured log's record bytes go: decodes every Nth record of a capture's log.bin (the
// contention driver's capture_log) with the format explorer's own decoder, and sums the bytes of
// each leaf field by its schema path (repeated fields' indices dropped), tags and lengths
// included in the field they frame. Each record is decoded alone, in a file of the log's header
// and checkpoint and that record.
//
// Usage (from fileio-catalog/):
//   node docs/format-explorer/anatomy.mjs [--every N] [--out DIR] <capture dir>...
//
// Writes DIR/<capture dir name>/ (DIR defaults to bench/results/anatomy) with manifest.json (the
// capture's label and config hash, the sample, record sizes) and fields.jsonl (one line per
// schema path: bytes over the sample, occurrences of the field, bytes per record), which lab/
// reads as the source `anatomy`.
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { join, dirname, basename, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';
import vm from 'node:vm';

const here = dirname(fileURLToPath(import.meta.url));
const args = process.argv.slice(2);
let every = 50;
let out = resolve(here, '../../bench/results/anatomy');
const captures = [];
for (let i = 0; i < args.length; i++) {
  if (args[i] === '--every') every = Number(args[++i]);
  else if (args[i] === '--out') out = resolve(args[++i]);
  else captures.push(resolve(args[i]));
}
if (!captures.length || !(every > 0)) {
  console.error('usage: anatomy.mjs [--every N] [--out DIR] <capture dir>...');
  process.exit(2);
}

const html = readFileSync(join(here, 'index.html'), 'utf8');
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

function git(...a) {
  try {
    return execFileSync('git', a, { cwd: here, encoding: 'utf8' }).trim();
  } catch {
    return null;
  }
}
const versions = {
  fileio_git: git('rev-parse', 'HEAD'),
  fileio_dirty: git('status', '--porcelain', '--', '.') !== '',
};

// a map entry's key: its field 1, a string
function mapKey(bytes, start, end) {
  if (bytes[start] !== 0x0a) return '?';
  let length = 0, shift = 0, at = start + 1, b;
  do { b = bytes[at++]; length += (b & 0x7f) * 2 ** shift; shift += 7; } while (b & 0x80);
  return at + length <= end ? new TextDecoder().decode(bytes.subarray(at, at + length)) : '?';
}

for (const dir of captures) {
  const started = new Date().toISOString();
  const capture = JSON.parse(readFileSync(join(dir, 'manifest.json'), 'utf8'));
  const buf = new Uint8Array(readFileSync(join(dir, 'log.bin')));
  const varint = (at) => {
    let v = 0, s = 0, b;
    do { b = buf[at++]; v += (b & 0x7f) * 2 ** s; s += 7; } while (b & 0x80);
    return [v, at];
  };
  // the header is 8 bytes; a length-prefixed checkpoint follows, then length-prefixed records
  const [checkpointLength, checkpointAt] = varint(8);
  const header = buf.slice(0, checkpointAt + checkpointLength);
  const fields = new Map();
  const sizes = [];
  let records = 0, recordBytes = 0, sampleBytes = 0, problems = 0;
  for (let at = header.length; at < buf.length;) {
    const start = at;
    const [length, body] = varint(at);
    const end = body + length;
    records++;
    recordBytes += end - start;
    if (records % every === 0) {
      const file = new Uint8Array(header.length + end - start);
      file.set(header, 0);
      file.set(buf.subarray(start, end), header.length);
      const decoded = explorer.decode(file);
      sizes.push(end - start);
      sampleBytes += end - start;
      let covered = 0;
      const leaves = decoded.nodes.filter(
        (n) => n.byteStart >= header.length && !(n.children && n.children.length));
      // a map's entries are split by key, path{key}: the entry's tag, length and bytes
      const keys = new Map();
      for (let i = 0; i < leaves.length; i++) {
        if (!(leaves[i].fieldType || '').startsWith('map<') || leaves[i].kind === 'tag'
            || leaves[i].kind === 'len') continue;
        const key = mapKey(file, leaves[i].byteStart, leaves[i].byteEnd);
        for (let j = i; j >= 0 && j > i - 3 && leaves[j].schemaPath === leaves[i].schemaPath; j--) {
          keys.set(leaves[j], key);
          if (leaves[j].kind === 'tag') break;
        }
      }
      for (const n of leaves) {
        let path = (n.schemaPath || n.label || '?').replace(/\[\d+\]/g, '');
        if (keys.has(n)) path += `{${keys.get(n)}}`;
        const f = fields.get(path) || { bytes: 0, count: 0 };
        f.bytes += n.byteEnd - n.byteStart;
        // a field decodes as its tag, its length (if delimited) and its value: count it once
        if (n.kind === 'tag' || n.kind === 'frame') f.count++;
        fields.set(path, f);
        covered += n.byteEnd - n.byteStart;
      }
      if (covered !== end - start) problems++;
    }
    at = end;
  }
  sizes.sort((a, b) => a - b);
  const q = (p) => sizes[Math.floor(p * (sizes.length - 1))];
  const sampled = sizes.length;
  const name = basename(dir);
  const target = join(out, name);
  mkdirSync(target, { recursive: true });
  writeFileSync(join(target, 'fields.jsonl'), [...fields.entries()]
    .sort((a, b) => b[1].bytes - a[1].bytes)
    .map(([path, f]) => JSON.stringify({ path, bytes: f.bytes, count: f.count,
      bytes_per_record: f.bytes / sampled }))
    .join('\n') + '\n');
  const summary = { records, record_bytes: recordBytes, sampled, sample_bytes: sampleBytes,
    mean: sampleBytes / sampled, p50: q(0.5), p90: q(0.9), p99: q(0.99), max: sizes[sampled - 1],
    uncovered_records: problems };
  writeFileSync(join(target, 'manifest.json'), JSON.stringify({
    label: capture.label, config_hash: capture.config_hash, started,
    finished: new Date().toISOString(), versions,
    config: { every, capture: name, capture_git: capture.versions?.fileio_git ?? null },
    summary }, null, 2) + '\n');
  console.log(`${name}: ${records} records, ${sampled} sampled, mean ${summary.mean.toFixed(0)} B`
    + (problems ? `, ${problems} not fully covered by leaves` : ''));
}
