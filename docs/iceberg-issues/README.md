# Iceberg core issues, one document each

Bugs in Apache Iceberg's core that fileio-catalog's fuzzers found. Each
document covers the bug, a reproduction on Iceberg's own `InMemoryCatalog`,
the cause with links to the 1.12.0 source, and the fix. It also lists the
fix's tests, the workaround fileio-catalog dropped, its canary in
`TestIcebergCoreIssues` that asserts the bug is absent, and what to settle
before a pull request upstream. [../ICEBERG_ISSUES.md](../ICEBERG_ISSUES.md)
tracks their status for rebases.

The fixes are on branch
[`core-fixes`](https://github.com/cdouglas/iceberg/tree/core-fixes) of the
fork, one commit each on the `apache-iceberg-1.12.0` tag. As of 2026-10-18,
each cherry-picks onto upstream `main` (`9bbee07`); only C2's test file
conflicts, and only if C7 isn't picked first.

| Issue | Fix | Before a PR |
|---|---|---|
| [C1. A delete that merges manifests gives existing rows new ids](C1.md) | upstream [#16263](https://github.com/apache/iceberg/pull/16263), in 1.11.0 | nothing to do |
| [C2. A retried commit gives a manifest a second row-id range](C2.md) | [`6c6b6fd`](https://github.com/cdouglas/iceberg/commit/6c6b6fd787d1a57ddfc789ce2ec7d0dc32a88582) | four caches: one PR or four |
| [C3. A replace transaction brings back a snapshot expired since it began](C3.md) | [`56527cb`](https://github.com/cdouglas/iceberg/commit/56527cb60aa7ca54d77cb0795d9a821b56800f7b) | coordinate with PR #16943 |
| [C4. A stale handle fails on a manifest in a newer partition spec](C4.md) | [`bfc20da`](https://github.com/cdouglas/iceberg/commit/bfc20da320f4b40715e17571dab16053637f33d4) | ready |
| [C5. Rewriting manifests fails on a table without a current snapshot](C5.md) | [`f723a52`](https://github.com/cdouglas/iceberg/commit/f723a52a663326cf5fd80d763df1a2d4988dc1f7) | empty snapshot or no commit |
| [C6. A repeated cherry-pick leaves a table it can't plan](C6.md) | [`724608a`](https://github.com/cdouglas/iceberg/commit/724608a099eec717e59b11cd09cb062a4dfceb28) | least certain: cost and semantics |
| [C7. The all_* metadata tables miss the row ids of an upgraded file](C7.md) | [`2779515`](https://github.com/cdouglas/iceberg/commit/27795151cf7ec998a6ee018e31f6cbe85b021cd1) | ready |
