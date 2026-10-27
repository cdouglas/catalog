"""Figures for papers and the website: `lab export NAME --to DIR` writes NAME.png (and .pdf,
.svg on request) plus NAME.md, the table twin. In a page, labbook.show("NAME") renders one."""

from labbook import figure, q  # noqa: F401


# @figure("runs-by-status", alt="Number of runs in each status")
# def runs_by_status():
#     import matplotlib.pyplot as plt
#
#     df = q("SELECT status, count(*) AS runs FROM runs GROUP BY status ORDER BY status")
#     fig, ax = plt.subplots(figsize=(4, 2.5))
#     ax.barh(df.status, df.runs)
#     ax.set_xlabel("runs")
#     return fig, df
