"""Figures for papers and the website: `lab export NAME --to DIR` writes NAME.png (and .pdf,
.svg on request) plus NAME.md, the table twin. In a page, labbook.show("NAME") renders one.

Also the helpers the pages share: figures_module().ENCODINGS, .encoding_color()."""

from labbook import figure, q, scale  # noqa: F401

# The conflict encodings, from the one that merges nothing to the one that merges most: every
# page orders and colors them so.
ENCODINGS = [
    "lamport",
    "lamport-minus-properties",
    "concern-vector",
    "blind-append",
    "partition-vector",
    "manifest-window",
]


def encoding_color(title="encoding", only=None):
    """An Altair color channel for the encoding column, in ENCODINGS' order and colors; only=[...]
    lists just those encodings in the legend, each keeping its color."""
    import altair as alt

    colors = scale(domain=ENCODINGS)
    if only is not None:
        keep = [i for i, e in enumerate(ENCODINGS) if e in only]
        colors = alt.Scale(domain=[ENCODINGS[i] for i in keep],
                           range=[colors.range[i] for i in keep])
    return alt.Color("encoding:N", title=title, sort=ENCODINGS, scale=colors)
