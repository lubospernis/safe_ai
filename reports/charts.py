"""Chart rendering functions — NBS brand style."""

import io
import re

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

# NBS brand palette
NBS_NAVY   = "#2B5291"
NBS_BLUE   = "#0086DE"
NBS_TEAL   = "#008C7A"
NBS_BURG   = "#A63559"
NBS_ORANGE = "#FF7430"
NBS_YELLOW = "#FAB937"
NBS_GREY   = "#D2DBE0"
NBS_TEXT   = "#231f20"

COUNTRIES = {"SK": "Slovakia", "EA": "Euro Area", "DE": "Germany"}
COUNTRY_COLORS = {"SK": NBS_NAVY, "EA": NBS_BLUE, "DE": NBS_TEAL}
COUNTRY_ORDER = ["SK", "EA", "DE"]

INSTRUMENT_COLORS = {
    "a": "#bd4e35", "b": "#0777b3", "f": "#e18727", "g": "#5a9e6f", "h": "#7b5ea7",
}
INSTRUMENT_LABELS = {
    "a": "Bank loans", "b": "Trade credit", "f": "Credit lines",
    "g": "Leasing/hire-purchase", "h": "Other loans",
}

# Chart text strings not derived from data — used as the English default. Pass a
# `labels` dict (see SK_LABELS) with the same keys to render a translated chart.
CHART_STRINGS = {
    "weighted_mean_pct":         "Weighted mean (%)",
    "need_suffix":               "need",
    "availability_suffix":       "availability",
    "gap_suffix":                "gap",
    "financing_gap_title_tmpl":  "{label} — need vs availability (bars); financing gap (dashed)",
    "sk_instrument_gap_title":   "Slovakia — financing gap by instrument (latest wave)",
    "question_prefix":           "Q",
    "net_change_pct_suffix":     " (net change in %)",
}

SK_LABELS = {
    "countries":    {"SK": "Slovensko", "EA": "Eurozóna", "DE": "Nemecko"},
    "instruments":  {
        "a": "Bankové úvery", "b": "Obchodný úver", "f": "Kreditné linky",
        "g": "Lízing/nákup na splátky", "h": "Ostatné úvery",
    },
    "strings": {
        "weighted_mean_pct":        "Vážený priemer (%)",
        "need_suffix":              "potreba",
        "availability_suffix":      "dostupnosť",
        "gap_suffix":               "medzera",
        "financing_gap_title_tmpl": "{label} — potreba vs. dostupnosť (stĺpce); medzera vo financovaní (čiarkovane)",
        "sk_instrument_gap_title":  "Slovensko — medzera vo financovaní podľa nástroja (posledná vlna)",
        "question_prefix":          "Ot",
        "net_change_pct_suffix":    " (čistá zmena v %)",
    },
}


def _resolve_labels(labels: dict | None) -> tuple[dict, dict, dict]:
    """Merge an optional override dict (see SK_LABELS shape) over the English
    defaults. Returns (countries, instruments, strings)."""
    labels = labels or {}
    countries = {**COUNTRIES, **labels.get("countries", {})}
    instruments = {**INSTRUMENT_LABELS, **labels.get("instruments", {})}
    strings = {**CHART_STRINGS, **labels.get("strings", {})}
    return countries, instruments, strings


def _nbs_style_ax(ax, chart_type: str, waves=None, xtick_labels=None, pct_axis: bool = False) -> None:
    """Apply NBS visual style to a single axes."""
    ax.set_facecolor("#f4f4f4")
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.tick_params(which="both", length=0, colors=NBS_TEXT)
    ax.title.set_fontsize(10)
    ax.title.set_color(NBS_TEXT)
    ax.yaxis.grid(True, color="#D2DBE0", linewidth=0.6, linestyle="-", zorder=0)
    ax.xaxis.grid(False)
    ax.set_axisbelow(True)
    ax.axhline(0, color="#9aa5ad", linewidth=0.8, zorder=1)
    if chart_type == "line" and waves is not None:
        ax.set_xticks(waves)
        ax.set_xticklabels(xtick_labels or [], rotation=35, ha="right", fontsize=8)
        fmt = "%+.0f%%" if pct_axis else "%+.0f"
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter(fmt))
    else:
        fmt = "%.1f%%" if pct_axis else "%.1f"
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter(fmt))


def _select_panels(sec: dict, df: pd.DataFrame, best_panel) -> list:
    """Return ordered list of panel values to plot, capped at max_panels."""
    panel_col = sec["panel_col"]
    if not panel_col:
        return [None]

    pinned = list(sec["pinned_panels"])
    all_panels = sorted(df[panel_col].dropna().unique().tolist(), key=str)

    if best_panel is not None and str(best_panel) not in [str(p) for p in pinned]:
        latest = df["wave_number"].max()
        value_col = sec["value_col"]
        panel_data = df[(df[panel_col].astype(str) == str(best_panel)) & (df["wave_number"] == latest)]
        countries_present = set(panel_data[panel_data[value_col].notna()]["country_code"].tolist())
        if {"SK", "EA", "DE"}.issubset(countries_present):
            pinned.append(best_panel)

    for p in all_panels:
        if len(pinned) >= sec["max_panels"]:
            break
        if str(p) not in [str(x) for x in pinned]:
            pinned.append(p)

    return pinned[: sec["max_panels"]]


_TITLE_FONTSIZE = 12.5
_QUESTION_FONTSIZE = 8.5


def _wrap_line_count(text: str, fontsize_pt: float, fig_w_in: float) -> int:
    """Rough estimate of how many lines matplotlib's `wrap=True` will produce for a
    centered string, so the figure can reserve enough vertical space up front."""
    if not text:
        return 0
    avg_char_in = fontsize_pt / 72.0 * 0.55
    usable_in = fig_w_in * 0.90
    chars_per_line = max(10, int(usable_in / avg_char_in))
    return max(1, -(-len(text) // chars_per_line))


def _title_block_layout(fig_w: float, fig_h: float, base_top: float,
                         chart_title: str, chart_question: str) -> tuple:
    """Return (top_margin, title_y, question_y) so a big chart_title + italic
    'Q: ...' chart_question caption fit above the axes without overlapping each
    other or the top row of panel titles — accounting for word-wrap on narrow
    (single-panel) figures, where a long title commonly spans 2 lines."""
    title_line_h = _TITLE_FONTSIZE / 72.0 * 1.3
    question_line_h = _QUESTION_FONTSIZE / 72.0 * 1.3
    title_in = _wrap_line_count(chart_title, _TITLE_FONTSIZE, fig_w) * title_line_h
    question_in = _wrap_line_count(chart_question, _QUESTION_FONTSIZE, fig_w) * question_line_h
    top_reserved_in = title_in + question_in + (0.05 if (chart_title or chart_question) else 0.0)
    top_margin = max(0.55, base_top - top_reserved_in / fig_h) if top_reserved_in else base_top
    title_y = 0.995
    question_y = 0.995 - title_in / fig_h - (0.015 if chart_title else 0.0)
    return top_margin, title_y, question_y


def build_chart(sec: dict, df: pd.DataFrame, chart_type: str, best_panel, chart_subtitle: str = "",
                 chart_title: str = "", chart_question: str = "", labels: dict | None = None,
                 panel_title_suffix: str = "", pct_axis: bool = False) -> bytes:
    countries, _instruments, strings = _resolve_labels(labels)
    panels = _select_panels(sec, df, best_panel)
    n_panels = len(panels)
    panel_col = sec["panel_col"]
    panel_label_col = sec.get("panel_label_col", panel_col)
    value_col = sec["value_col"]
    series_col = sec["series_col"]

    ncols = min(n_panels, 2)
    nrows = (n_panels + 1) // 2

    if n_panels == 1:
        fig_w, fig_h = 5.0, 3.2
    else:
        fig_w, fig_h = 4.5 * ncols, 3.2 * nrows

    sharey_mode = "row" if (chart_type == "bar" and n_panels > 1) else False
    fig, axes = plt.subplots(nrows, ncols, figsize=(fig_w, fig_h), sharey=sharey_mode)
    if n_panels == 1:
        axes_flat = [axes]
    else:
        axes_flat = list(np.array(axes).flatten())

    bottom_margin = 0.30 if chart_subtitle else 0.22
    top_margin, title_y, question_y = _title_block_layout(fig_w, fig_h, 0.86, chart_title, chart_question)
    fig.subplots_adjust(top=top_margin, hspace=0.70, wspace=0.30, bottom=bottom_margin)
    fig.patch.set_facecolor("#f4f4f4")

    waves = sorted(df["wave_number"].unique())
    wave_labels = (
        df[["wave_number", "survey_period_label"]]
        .drop_duplicates(subset=["wave_number"])
        .sort_values("wave_number")
        .set_index("wave_number")["survey_period_label"]
    )
    xtick_labels = [str(wave_labels[w]) for w in waves]

    handles, legend_labels = [], []

    for ax, panel_val in zip(axes_flat, panels):
        if panel_col and panel_val is not None:
            sub_df = df[df[panel_col].astype(str) == str(panel_val)]
            label_val = sub_df[panel_label_col].iloc[0] if not sub_df.empty and panel_label_col in sub_df else str(panel_val)
        else:
            sub_df = df
            label_val = sec["title"]

        if chart_type == "bar":
            latest_wave = df["wave_number"].max()
            bar_df = sub_df[sub_df["wave_number"] == latest_wave]
            x = np.arange(len(COUNTRY_ORDER))
            width = 0.52
            for i, country in enumerate(COUNTRY_ORDER):
                cdf = bar_df[bar_df[series_col] == country]
                val = cdf[value_col].iloc[0] if not cdf.empty else 0
                bar = ax.bar(x[i], val, width, color=COUNTRY_COLORS[country],
                             edgecolor="none", zorder=2)
                if panel_val == panels[0]:
                    handles.append(bar)
                    legend_labels.append(countries[country])
            ax.set_xticks(x)
            ax.set_xticklabels([countries[c] for c in COUNTRY_ORDER], fontsize=8)
            ax.axhline(0, color="#D2DBE0", linewidth=0.9, linestyle="-", zorder=0)
        else:
            for country in COUNTRY_ORDER:
                cdf = sub_df[sub_df[series_col] == country].sort_values("wave_number")
                if cdf.empty:
                    continue
                line, = ax.plot(
                    cdf["wave_number"],
                    cdf[value_col],
                    label=countries[country],
                    color=COUNTRY_COLORS[country],
                    linewidth=2,
                    marker="o",
                    markersize=4,
                )
                if panel_val == panels[0]:
                    handles.append(line)
                    legend_labels.append(countries[country])

        ax.set_title(f"{label_val}{panel_title_suffix}", fontsize=9, pad=6, wrap=True)
        ax.set_ylabel("")
        _nbs_style_ax(ax, chart_type,
                      waves=(waves if chart_type == "line" else None),
                      xtick_labels=(xtick_labels if chart_type == "line" else None),
                      pct_axis=pct_axis)

    for ax in axes_flat[n_panels:]:
        ax.set_visible(False)

    fig.legend(
        handles, legend_labels,
        loc="lower center",
        bbox_to_anchor=(0.5, 0.01),
        ncol=len(COUNTRY_ORDER),
        fontsize=9,
        frameon=False,
        handlelength=1.0,
        handleheight=0.8,
    )

    if chart_title:
        fig.suptitle(chart_title, fontsize=_TITLE_FONTSIZE, fontweight="bold", color=NBS_TEXT,
                     y=title_y, wrap=True)
    if chart_question:
        fig.text(0.5, question_y, f"{strings['question_prefix']}: {chart_question}", ha="center", va="top",
                 fontsize=_QUESTION_FONTSIZE, style="italic", color="#5a5a5a", wrap=True)

    if chart_subtitle:
        fig.text(0.5, 0.005, chart_subtitle, ha="center", va="bottom",
                 fontsize=7.5, color=NBS_TEXT, style="italic", wrap=True)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="#f4f4f4")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def _financing_gap_bars(df: pd.DataFrame, chart_title: str = "", chart_question: str = "",
                         labels: dict | None = None) -> bytes:
    """Grouped bars (need/availability) + gap line for bank loans (sub_item='a')."""
    import matplotlib.colors as mcolors

    countries, instruments, strings = _resolve_labels(labels)
    sub_df = df[df["sub_item"] == "a"].copy()
    label_val = sub_df["sub_item_label"].iloc[0] if not sub_df.empty else instruments["a"]

    waves = sorted(sub_df["wave_number"].unique())
    wave_labels = (
        sub_df[["wave_number", "survey_period_label"]]
        .drop_duplicates(subset=["wave_number"]).sort_values("wave_number")
        .set_index("wave_number")["survey_period_label"]
    )

    fig_w, fig_h = 7.5, 3.8
    fig, ax = plt.subplots(1, 1, figsize=(fig_w, fig_h))
    fig.patch.set_facecolor("#f4f4f4")
    top_margin, title_y, question_y = _title_block_layout(fig_w, fig_h, 0.82, chart_title, chart_question)
    fig.subplots_adjust(top=top_margin, bottom=0.26, left=0.09, right=0.97)

    n_countries = len(COUNTRY_ORDER)
    group_gap = 1.0
    bar_width = 0.7 / (n_countries * 2)
    bar_handles, bar_labels_leg, line_handles, line_labels_leg = [], [], [], []

    for c_idx, country in enumerate(COUNTRY_ORDER):
        cdf = sub_df[sub_df["country_code"] == country].sort_values("wave_number")
        if cdf.empty:
            continue
        base_color = COUNTRY_COLORS[country]
        rgb = mcolors.to_rgb(base_color)
        light_color = tuple(min(1.0, v + 0.30) for v in rgb)

        for w_idx, wave in enumerate(waves):
            row = cdf[cdf["wave_number"] == wave]
            if row.empty:
                continue
            pair_offset = (c_idx - n_countries / 2 + 0.5) * (2 * bar_width + 0.02)
            x_center = w_idx * group_gap
            b1 = ax.bar(x_center + pair_offset, row["need_nb"].iloc[0], bar_width,
                        color=base_color, edgecolor="white", linewidth=0.5, zorder=2)
            b2 = ax.bar(x_center + pair_offset + bar_width, row["availability_nb"].iloc[0], bar_width,
                        color=light_color, hatch="//", edgecolor=base_color, linewidth=0.5, zorder=2)
            if w_idx == 0:
                bar_handles += [b1, b2]
                bar_labels_leg += [
                    f"{countries[country]} — {strings['need_suffix']}",
                    f"{countries[country]} — {strings['availability_suffix']}",
                ]

        x_pts = [i * group_gap for i, w in enumerate(waves) if not cdf[cdf["wave_number"] == w].empty]
        gap_vals = [cdf[cdf["wave_number"] == w]["financing_gap_wtd"].iloc[0] for w in waves
                    if not cdf[cdf["wave_number"] == w].empty]
        line, = ax.plot(x_pts, gap_vals, color=base_color, linewidth=2.0,
                        marker="D", markersize=4, linestyle="--", zorder=3)
        line_handles.append(line)
        line_labels_leg.append(f"{countries[country]} — {strings['gap_suffix']}")

    ax.set_facecolor("#f4f4f4")
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.tick_params(which="both", length=0, colors=NBS_TEXT)
    ax.yaxis.grid(True, color="#D2DBE0", linewidth=0.6, linestyle="-", zorder=0)
    ax.xaxis.grid(False)
    ax.set_axisbelow(True)
    ax.axhline(0, color="#9aa5ad", linewidth=0.8, zorder=1)
    ax.set_xticks([i * group_gap for i in range(len(waves))])
    ax.set_xticklabels([str(wave_labels[w]) for w in waves], rotation=35, ha="right", fontsize=8)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%+.0f"))
    ax.set_ylabel("")
    ax.set_title(strings["financing_gap_title_tmpl"].format(label=label_val), fontsize=9)
    fig.legend(bar_handles + line_handles, bar_labels_leg + line_labels_leg,
               loc="lower center", bbox_to_anchor=(0.5, 0.0), ncol=3, fontsize=7.5, frameon=False)

    if chart_title:
        fig.suptitle(chart_title, fontsize=_TITLE_FONTSIZE, fontweight="bold", color=NBS_TEXT,
                     y=title_y, wrap=True)
    if chart_question:
        fig.text(0.5, question_y, f"{strings['question_prefix']}: {chart_question}", ha="center", va="top",
                 fontsize=_QUESTION_FONTSIZE, style="italic", color="#5a5a5a", wrap=True)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="#f4f4f4")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def _financing_gap_sk_instruments(df_sk: pd.DataFrame, labels: dict | None = None) -> bytes:
    """Financing gap for Slovakia by instrument — bar chart, latest wave only."""
    _countries, instrument_labels, strings = _resolve_labels(labels)
    latest_wave = df_sk["wave_number"].max()
    bar_df = df_sk[df_sk["wave_number"] == latest_wave].copy()

    instruments = [s for s in INSTRUMENT_COLORS if not bar_df[bar_df["sub_item"] == s].empty]
    x = np.arange(len(instruments))
    vals, colors, tick_labels = [], [], []
    for sub_item in instruments:
        row = bar_df[bar_df["sub_item"] == sub_item]
        vals.append(row["financing_gap_wtd"].iloc[0] if not row.empty else 0)
        colors.append(INSTRUMENT_COLORS[sub_item])
        tick_labels.append(
            row["sub_item_label"].iloc[0] if ("sub_item_label" in row.columns and not row.empty)
            else instrument_labels.get(sub_item, sub_item)
        )

    fig, ax = plt.subplots(1, 1, figsize=(5.5, 3.2))
    fig.patch.set_facecolor("#f4f4f4")
    fig.subplots_adjust(top=0.86, bottom=0.30, left=0.10, right=0.97)

    ax.bar(x, vals, 0.55, color=colors, edgecolor="none", zorder=2)
    ax.set_xticks(x)
    ax.set_xticklabels(tick_labels, fontsize=8, rotation=25, ha="right")
    ax.set_facecolor("#f4f4f4")
    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.tick_params(which="both", length=0, colors=NBS_TEXT)
    ax.yaxis.grid(True, color="#D2DBE0", linewidth=0.6, linestyle="-", zorder=0)
    ax.xaxis.grid(False)
    ax.set_axisbelow(True)
    ax.axhline(0, color="#9aa5ad", linewidth=0.8, zorder=1)
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%+.0f"))
    ax.set_ylabel("")
    ax.set_title(strings["sk_instrument_gap_title"], fontsize=9)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="#f4f4f4")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def build_financing_gap_chart(sec: dict, df: pd.DataFrame, chart_title: str = "",
                               chart_question: str = "", labels: dict | None = None) -> bytes:
    """Returns a single PNG stacking the need/availability chart and SK instrument chart."""
    df_main = df[df["chart_type"] == "main"]
    df_sk = df[df["chart_type"] == "sk_all"]

    png1 = _financing_gap_bars(df_main, chart_title=chart_title, chart_question=chart_question, labels=labels)
    png2 = _financing_gap_sk_instruments(df_sk, labels=labels)

    from PIL import Image
    img1 = Image.open(io.BytesIO(png1))
    img2 = Image.open(io.BytesIO(png2))
    combined = Image.new("RGB", (max(img1.width, img2.width), img1.height + img2.height), (248, 248, 248))
    combined.paste(img1, (0, 0))
    combined.paste(img2, (0, img1.height))
    buf = io.BytesIO()
    combined.save(buf, format="PNG")
    buf.seek(0)
    return buf.read()


def _adhoc_chart_caption(theme: dict) -> str:
    """Caption shown above an adhoc chart's panels: prefer the LLM-shortened
    question (theme['chart_question'], set by get_shortened_questions() in
    llm.py), fall back to the raw annex question text truncated to 110 chars."""
    short_q = (theme.get("chart_question") or "").strip()
    if short_q:
        return short_q
    q_text = (theme.get("question_text") or "").strip()
    if q_text:
        q_text = re.sub(r"^[-–•]\s*", "", q_text).strip()
        return q_text[:110]
    return ""


def _build_adhoc_chart(
    df: pd.DataFrame,
    theme: dict,
    is_continuous: bool = False,
    response_labels: dict | None = None,
    labels: dict | None = None,
) -> bytes | None:
    """Render NBS-styled bar chart from adhoc chart DataFrame. Returns PNG bytes or None."""
    if df is None or df.empty:
        return None
    try:
        if is_continuous:
            return _build_adhoc_chart_continuous(df, theme, labels=labels)
        return _build_adhoc_chart_categorical(df, theme, response_labels, labels=labels)
    except Exception as e:
        print(f"  Adhoc chart render failed: {e}")
        return None


def _build_adhoc_chart_continuous(df: pd.DataFrame, theme: dict, labels: dict | None = None) -> bytes | None:
    """For continuous modules: grouped bar chart with countries on x-axis, mean % on y-axis."""
    countries, _instruments, strings = _resolve_labels(labels)
    sub_items = sorted(df["sub_item"].unique())
    n_panels = len(sub_items)
    ncols = min(n_panels, 2)
    nrows = (n_panels + 1) // 2
    fig_w = 4.5 if n_panels == 1 else 4.5 * ncols
    fig_h = 3.4 if n_panels == 1 else 3.4 * nrows

    fig, axes = plt.subplots(nrows, ncols, figsize=(fig_w, fig_h))
    axes_flat = [axes] if n_panels == 1 else list(np.array(axes).flatten())
    fig.patch.set_facecolor("#f4f4f4")
    fig.subplots_adjust(top=0.86, hspace=0.60, wspace=0.35, bottom=0.18)

    countries_in_data = [c for c in COUNTRY_ORDER if c in df["country_code"].values]
    handles, legend_labels_list = [], []

    for ax, sub in zip(axes_flat, sub_items):
        sub_df = df[df["sub_item"] == sub].copy()
        # Compute weighted mean per country
        means = {}
        for country in countries_in_data:
            cdf = sub_df[sub_df["country_code"] == country]
            if cdf.empty:
                continue
            # Use n_firms_wtd if available, else n_firms as proxy
            w_col = "n_firms_wtd" if "n_firms_wtd" in cdf.columns else "n_firms"
            total_w = cdf[w_col].sum()
            if total_w > 0:
                means[country] = (cdf["response_raw"] * cdf[w_col]).sum() / total_w

        x = np.arange(len(means))
        country_keys = list(means.keys())
        vals = [means[c] for c in country_keys]
        colors = [COUNTRY_COLORS.get(c, "#888") for c in country_keys]

        bars = ax.bar(x, vals, width=0.5, color=colors, edgecolor="none", zorder=2)
        # Value labels on top of bars
        for bar, val in zip(bars, vals):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
                    f"{val:.1f}%", ha="center", va="bottom", fontsize=7, color=NBS_TEXT)

        if sub == sub_items[0]:
            for bar, country in zip(bars, country_keys):
                handles.append(bar)
                legend_labels_list.append(countries.get(country, country))

        ax.set_xticks(x)
        ax.set_xticklabels([countries.get(c, c) for c in country_keys], fontsize=8)
        ax.set_ylabel(strings["weighted_mean_pct"], fontsize=7.5)

        # Panel title from question_texts or theme label
        panel_title = str(
            (theme.get("question_texts") or {}).get(sub, sub or theme["theme_label"])
        )
        ax.set_title(panel_title[:55], fontsize=8, pad=5)
        _nbs_style_ax(ax, "bar")

    for ax in axes_flat[n_panels:]:
        ax.set_visible(False)

    caption = _adhoc_chart_caption(theme)
    if caption:
        fig.suptitle(caption, fontsize=7, color="#666666", style="italic",
                     y=1.01, ha="center", wrap=True)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="#f4f4f4")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def _build_adhoc_chart_categorical(
    df: pd.DataFrame,
    theme: dict,
    response_labels: dict | None = None,
    labels: dict | None = None,
) -> bytes | None:
    """For categorical modules: grouped bars by response code, SK vs EA per panel."""
    country_names, _instruments, _strings = _resolve_labels(labels)
    sub_items = sorted(df["sub_item"].unique())
    n_panels = len(sub_items)
    ncols = min(n_panels, 2)
    nrows = (n_panels + 1) // 2
    fig_w = 5.0 if n_panels == 1 else 4.5 * ncols
    fig_h = 3.2 if n_panels == 1 else 3.2 * nrows

    fig, axes = plt.subplots(nrows, ncols, figsize=(fig_w, fig_h))
    axes_flat = [axes] if n_panels == 1 else list(np.array(axes).flatten())
    fig.patch.set_facecolor("#f4f4f4")
    fig.subplots_adjust(top=0.86, hspace=0.70, wspace=0.30, bottom=0.22)

    countries = [c for c in ["SK", "EA", "DE"] if c in df["country_code"].values]
    handles, legend_labels_list = [], []

    flat_labels: dict[int, str] = {}
    if response_labels:
        mid = (theme.get("module_id") or "").lower()
        flat_labels = dict(response_labels.get(mid, {}))
        if not flat_labels:
            # fallback: merge all modules only when this module's labels aren't found
            for module_labels in response_labels.values():
                flat_labels.update(module_labels)

    for ax, sub in zip(axes_flat, sub_items):
        sub_df = df[df["sub_item"] == sub]
        x_vals = sorted(sub_df["response_raw"].unique())
        x = np.arange(len(x_vals))
        width = 0.35

        for i, country in enumerate(countries):
            cdf = sub_df[sub_df["country_code"] == country]
            vals = [
                cdf[cdf["response_raw"] == v]["pct_wtd"].iloc[0]
                if not cdf[cdf["response_raw"] == v].empty else 0
                for v in x_vals
            ]
            offset = (i - len(countries) / 2 + 0.5) * width
            bar = ax.bar(x + offset, vals, width,
                         color=COUNTRY_COLORS.get(country, "#888"),
                         edgecolor="none", zorder=2)
            if sub == sub_items[0]:
                handles.append(bar)
                legend_labels_list.append(country_names.get(country, country))

        ax.set_xticks(x)
        tick_labels = [flat_labels.get(int(v), str(int(v))) for v in x_vals]
        ax.set_xticklabels(tick_labels, rotation=35, ha="right", fontsize=7.5)

        label_col_vals = sub_df["sub_item_label"].dropna() if "sub_item_label" in sub_df.columns else None
        if label_col_vals is not None and not label_col_vals.empty:
            panel_title = str(label_col_vals.iloc[0])
        else:
            panel_title = str(
                (theme.get("question_texts") or {}).get(sub, sub or theme["theme_label"])
            )
        ax.set_title(panel_title[:55], fontsize=8, pad=5)
        _nbs_style_ax(ax, "bar")

    for ax in axes_flat[n_panels:]:
        ax.set_visible(False)

    fig.legend(handles, legend_labels_list, loc="lower center",
               bbox_to_anchor=(0.5, 0.01), ncol=len(countries),
               fontsize=9, frameon=False, handlelength=1.0)

    caption = _adhoc_chart_caption(theme)
    if caption:
        fig.suptitle(caption, fontsize=7, color="#666666", style="italic",
                     y=1.01, ha="center", wrap=True)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="#f4f4f4")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def _build_ai_chart(df: pd.DataFrame, title: str, is_continuous: bool = False,
                    label_col: str = "response_label") -> bytes | None:
    """Render NBS-styled bar chart for one AI sub-section. Returns PNG bytes or None."""
    if df is None or df.empty:
        return None
    try:
        sub_items = sorted(df["sub_item"].unique()) if "sub_item" in df.columns else [""]
        n_panels = len(sub_items)
        ncols = min(n_panels, 2)
        nrows = (n_panels + 1) // 2
        fig_w = 5.5 if n_panels == 1 else 4.5 * ncols
        fig_h = 3.4 if n_panels == 1 else 3.4 * nrows

        fig, axes = plt.subplots(nrows, ncols, figsize=(fig_w, fig_h))
        axes_flat = [axes] if n_panels == 1 else list(np.array(axes).flatten())
        fig.patch.set_facecolor("#f4f4f4")
        fig.subplots_adjust(top=0.84, hspace=0.72, wspace=0.30, bottom=0.24)

        countries = [c for c in ["SK", "EA"] if c in df["country_code"].values]
        handles, legend_labels_list = [], []

        for ax, sub in zip(axes_flat, sub_items):
            sub_df = df[df["sub_item"] == sub] if sub != "" else df
            x_vals = sorted(sub_df["response_raw"].unique())
            x = np.arange(len(x_vals))
            width = 0.35

            for i, country in enumerate(countries):
                cdf = sub_df[sub_df["country_code"] == country]
                vals = [
                    cdf[cdf["response_raw"] == v]["pct_wtd"].iloc[0]
                    if not cdf[cdf["response_raw"] == v].empty else 0
                    for v in x_vals
                ]
                offset = (i - len(countries) / 2 + 0.5) * width
                bar = ax.bar(x + offset, vals, width,
                             color=COUNTRY_COLORS.get(country, "#888"),
                             edgecolor="none", zorder=2)
                if sub == sub_items[0]:
                    handles.append(bar)
                    legend_labels_list.append(COUNTRIES.get(country, country))

            ax.set_xticks(x)
            if is_continuous:
                ax.set_xticklabels([f"{int(v)}–{int(v)+9}%" for v in x_vals],
                                   rotation=35, ha="right", fontsize=7.5)
            else:
                sk_rows = sub_df[sub_df["country_code"] == "SK"] if "SK" in sub_df["country_code"].values else sub_df
                label_map = {}
                for _, r in sk_rows.iterrows():
                    label_map[r["response_raw"]] = (r.get(label_col) or str(int(r["response_raw"])))
                ax.set_xticklabels(
                    [label_map.get(v, str(v)) for v in x_vals],
                    rotation=35, ha="right", fontsize=7.5
                )

            panel_title = title if n_panels == 1 else str(
                (sub_df.get("sub_item_label", sub_df["sub_item"]).iloc[0] if not sub_df.empty else sub)
            )[:55]
            ax.set_title(panel_title, fontsize=8, pad=5)
            _nbs_style_ax(ax, "bar")

        for ax in axes_flat[n_panels:]:
            ax.set_visible(False)

        fig.legend(handles, legend_labels_list, loc="lower center",
                   bbox_to_anchor=(0.5, 0.01), ncol=len(countries),
                   fontsize=9, frameon=False, handlelength=1.0)

        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="#f4f4f4")
        plt.close(fig)
        buf.seek(0)
        return buf.read()
    except Exception as e:
        print(f"  AI chart render failed: {e}")
        return None
