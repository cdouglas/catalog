---
kernelspec:
  name: labbook
  display_name: Python (labbook)
  language: python
title: Explore
---

```{code-cell} ipython3
:tags: [remove-cell]

import altair as alt
from labbook import q, table, theme, scale
theme()
```

What this page answers, in one sentence.

```{code-cell} ipython3
:label: explore-runs
:tags: [remove-input]

df = q("SELECT status, count(*) AS runs FROM runs GROUP BY status ORDER BY status")
alt.Chart(df, title="Runs by status").mark_bar().encode(
    x=alt.X("runs:Q", title="runs"), y=alt.Y("status:N", title=None))
```

```{code-cell} ipython3
:tags: [remove-input]

table(df)
```
