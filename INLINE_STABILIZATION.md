# Inline TM/ML stabilization — open items

Inline-TM and inline-ML are green across the cloud matrix — see
[docs/COMPAT.md](docs/COMPAT.md). Deferred functionality lives in
[docs/errata.md](docs/errata.md).

This file tracks inline TM/ML work still ahead.

## Open

### M2. Statistics-file changes force full mode (perf, not correctness)

`InlineDeltaCodec.computeDelta` returns `null` whenever a stats file
changed (line 774–776). Any commit that runs `setStatistics` in addition
to a real change pays full-mode bytes, defeating the delta-mode benefit
on tables that maintain stats. Adding `AddStatistics` /
`RemoveStatistics` delta types is straightforward; the JSON parsers are
already in upstream Iceberg.
