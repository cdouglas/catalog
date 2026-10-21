# Iceberg core issues, one document each

Bugs in Apache Iceberg's core that fileio-catalog's fuzzers found, and,
from C15 on, a review of Iceberg's commit retry paths that their finds
prompted. Each
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
conflicts, and only if C7 isn't picked first, and C8's, if C6 isn't. C9,
added 2026-10-19, cherry-picks onto `main` at `6890694bd` after C3; C10,
added the same day, onto `ddbd99073` on its own. C11, added 2026-10-20,
onto `3738f4a97` on its own, C12, the same day, after C10, and C13 after
C10 and C12; C14 after C6 and C8. C15 to C22, from the review, onto
`834441b96`: C16, C17, C19, C21 and C22 on their own, C15 after C10, C12
and C13, and C18 and C20 after C11.

| Issue | Fix | Before a PR |
|---|---|---|
| [C1. A delete that merges manifests gives existing rows new ids](C1.md) | upstream [#16263](https://github.com/apache/iceberg/pull/16263), in 1.11.0 | nothing to do |
| [C2. A retried commit gives a manifest a second row-id range](C2.md) | [`6c6b6fd`](https://github.com/cdouglas/iceberg/commit/6c6b6fd787d1a57ddfc789ce2ec7d0dc32a88582) | four caches: one PR or four |
| [C3. A replace transaction brings back a snapshot expired since it began](C3.md) | [`56527cb`](https://github.com/cdouglas/iceberg/commit/56527cb60aa7ca54d77cb0795d9a821b56800f7b) | coordinate with PR #16943 |
| [C4. A stale handle fails on a manifest in a newer partition spec](C4.md) | [`bfc20da`](https://github.com/cdouglas/iceberg/commit/bfc20da320f4b40715e17571dab16053637f33d4) | ready |
| [C5. Rewriting manifests fails on a table without a current snapshot](C5.md) | [`f723a52`](https://github.com/cdouglas/iceberg/commit/f723a52a663326cf5fd80d763df1a2d4988dc1f7) | empty snapshot or no commit |
| [C6. A repeated cherry-pick leaves a table it can't plan](C6.md) | [`724608a`](https://github.com/cdouglas/iceberg/commit/724608a099eec717e59b11cd09cb062a4dfceb28) | least certain: cost and semantics |
| [C7. The all_* metadata tables miss the row ids of an upgraded file](C7.md) | [`2779515`](https://github.com/cdouglas/iceberg/commit/27795151cf7ec998a6ee018e31f6cbe85b021cd1) | ready |
| [C8. A deletion vector older than its data file leaves a table Iceberg can't plan](C8.md) | [`1d46241`](https://github.com/cdouglas/iceberg/commit/1d46241c7cb1693801b1f6b2dc5300e233e5495e) | replaces an upstream test's assertion |
| [C9. A create-or-replace replaces a table created since it began with a different one](C9.md) | [`b7a2abd`](https://github.com/cdouglas/iceberg/commit/b7a2abd8ff75f5f00681c5ac4e24f0b0d99b3a36) | coordinate with PR #16943, which turns the race into a replace |
| [C10. An expiry that retries after a conflict fails on what its first attempt removed](C10.md) | [`8900018`](https://github.com/cdouglas/iceberg/commit/8900018d2405b5343eb676a8953da40df21a4112) | ready; optionally also guard the builder's list size |
| [C11. An append commits files of a partition spec another writer removed](C11.md) | [`69e83ef`](https://github.com/cdouglas/iceberg/commit/69e83effcbc7c59ec1c2822c6af38605d445501f) | the REST window needs a requirement or a server-side check |
| [C12. An expiry follows the GC and retention settings its table had when it was created](C12.md) | [`0824e0d`](https://github.com/cdouglas/iceberg/commit/0824e0d62d00c16e4015b5ba61044b599f4f7b79) | ready; settle whether `now` should be per attempt |
| [C13. An expiry that committed reports a failure when its file cleanup fails](C13.md) | [`7f3708a`](https://github.com/cdouglas/iceberg/commit/7f3708a2219a897bf5f5df81a39e845ea72c9580) | ready; the incremental-cleanup checks have the same flaw |
| [C14. A cherry-pick of a snapshot expired since it was picked fails with a NullPointerException](C14.md) | [`b1f2e01`](https://github.com/cdouglas/iceberg/commit/b1f2e01e4d761d2725adbcd3a8896b6f7ada07ab) | ready |
| [C15. An expiry in a transaction deletes files before the transaction commits](C15.md) | [`44fce44`](https://github.com/cdouglas/iceberg/commit/44fce44fa44468738dd6a56316307aedcf764453) | ready; cite the #13631 regression |
| [C16. A rollback undoes another writer's rollback, or fails with an NPE when its target expires](C16.md) | [`14d869e`](https://github.com/cdouglas/iceberg/commit/14d869e9d4d0c135c736f775d26dbea9ce49f735) | ready |
| [C17. Location and partition statistics updates fail after one concurrent commit](C17.md) | [`b0669b6`](https://github.com/cdouglas/iceberg/commit/b0669b67740742fdf9c56511d80ccb46557731ff) | coordinate with PR #18204 (partition statistics) |
| [C18. A replace of partitions, an overwrite checking its filter, or a merge of appended manifests fails with an NPE on a removed spec](C18.md) | [`4692c71`](https://github.com/cdouglas/iceberg/commit/4692c719a5a8d796fa99b62518839ea63ba58360) | fold into C11's PR |
| [C19. A sort order replaced after another writer renamed its column fails with an NPE](C19.md) | [`7e4517f`](https://github.com/cdouglas/iceberg/commit/7e4517f5d6acdccfcb2c60ae070e56d09ffba9a5) | ready; succeed (as REST does) or fail |
| [C20. A row delta whose starting snapshot expired fails with IllegalArgumentException](C20.md) | [`b967a0a`](https://github.com/cdouglas/iceberg/commit/b967a0a857788117019d5dba37e4b2faddfda0b2) | ready |
| [C21. Ref and spec removals applied to newer metadata lose a concurrent commit or remove a spec in use](C21.md) | [`517c1c4`](https://github.com/cdouglas/iceberg/commit/517c1c4a882f0b9613093947d5ed2bb92c35bac1) | more expiry conflicts with main; new branches still a gap |
| [C22. A snapshot removal applied to newer metadata drops a ref set since](C22.md) | [`a2a7847`](https://github.com/cdouglas/iceberg/commit/a2a78474660be2204654565b40d2874344b8a7e5) | check other clients' `remove-snapshots` |
