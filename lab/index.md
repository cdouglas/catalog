---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: fileio-catalog results
---

```{code-cell} ipython3
:tags: [remove-cell]

from labbook import q, table, meta, data_version
```

Every run labbook found, newest first. Superseded and excluded runs are listed here but left out
of the other pages, which read `canonical_runs`.

```{code-cell} ipython3
:label: run-catalog
:tags: [remove-input]
:class: col-page-right

promoted = "".join(f', "{c}"' for c in meta()["promoted"])
table(q(f"""
    SELECT run_id, status, coalesce(mark, '') AS mark, campaigns, tags,
           strftime(started_at, '%Y-%m-%d %H:%M') AS started,
           left(git_sha, 10) || CASE WHEN git_dirty THEN '*' ELSE '' END AS git{promoted}
    FROM runs ORDER BY started_at DESC NULLS LAST"""))
```

```{code-cell} ipython3
:label: campaigns
:tags: [remove-input]
:class: col-page-right

table(q("""
    SELECT c.campaign, c.title, count(r.run_id) AS runs,
           count(r.run_id) FILTER (WHERE r.status = 'finished') AS finished
    FROM campaigns c
    LEFT JOIN campaign_runs cr USING (campaign)
    LEFT JOIN runs r USING (run_id)
    GROUP BY ALL ORDER BY c.campaign"""), caption="Campaigns")
```

```{code-cell} ipython3
:tags: [remove-input]

print(f"data version {data_version()}, ingested {meta()['ingested_at']}")
```
