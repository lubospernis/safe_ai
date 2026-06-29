"""
SAFE Survey Report Generator
Runs Q10 SQL, generates chart, calls Claude for bullets, writes HTML to reports/output/.

Required environment variables:
  MOTHERDUCK_TOKEN   — MotherDuck service token
  ANTHROPIC_API_KEY  — Anthropic API key
"""

import base64
import io
import json
import os
import textwrap
from datetime import date
from pathlib import Path

import anthropic
import duckdb
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

matplotlib.use("Agg")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SQL_DIR = Path(__file__).parent / "sql"
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

COUNTRIES = {"SK": "Slovakia", "EA": "Euro Area", "AT": "Austria", "DE": "Germany"}
COUNTRY_COLORS = {"SK": "#bd4e35", "EA": "#0777b3", "AT": "#2d7a00", "DE": "#e18727"}

# sub_item 'a' (interest rates) is always shown regardless of interest check
ALWAYS_SHOW = {"a"}

# Wave filter is applied in q10.sql; this constant is kept for reference only
CHART_WAVE_MIN = 34

MOTHERDUCK_TOKEN = os.environ["MOTHERDUCK_TOKEN"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]


# ---------------------------------------------------------------------------
# 1. Fetch data
# ---------------------------------------------------------------------------

def fetch_data() -> pd.DataFrame:
    sql = (SQL_DIR / "q10.sql").read_text()
    con = duckdb.connect(f"md:my_db?motherduck_token={MOTHERDUCK_TOKEN}")
    df = con.execute(sql).df()
    con.close()
    return df


# ---------------------------------------------------------------------------
# 2. Interest check — decide which sub-items are worth showing
# ---------------------------------------------------------------------------

INTEREST_SYSTEM = textwrap.dedent("""
    You are a statistical analyst. Given Q10 net balance time series data for several
    countries, decide whether each sub-item is "interesting" enough to show in a chart.

    A sub-item is INTERESTING if, across any country, there is:
    - A swing of >=8pp between any two consecutive waves, OR
    - A clear monotonic trend (same direction for 3+ consecutive waves), OR
    - A notable divergence between countries (>=10pp gap in the latest wave).

    A sub-item is NOT INTERESTING if all series are flat (range <5pp across all waves
    and countries) or too noisy with no clear direction.

    Respond with a JSON object mapping sub_item letter to true/false.
    Example: {"a": true, "b": false, "c": true, "d": false, "e": true, "f": false}
    No explanation, just the JSON object.
""").strip()


def check_interest(df: pd.DataFrame) -> set[str]:
    """Return set of sub_item letters that are interesting enough to plot."""
    lines = []
    for si, grp in df.groupby("sub_item"):
        label = grp["sub_item_label"].iloc[0]
        lines.append(f"sub_item={si} ({label}):")
        for country in ["SK", "EA", "AT", "DE"]:
            cdf = grp[grp["country_code"] == country].sort_values("wave_number")
            if cdf.empty:
                continue
            vals = " ".join(f"w{r['wave_number']}:{r['net_balance_wtd']:+.1f}" for _, r in cdf.iterrows())
            lines.append(f"  {country}: {vals}")

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    msg = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=100,
        system=INTEREST_SYSTEM,
        messages=[{"role": "user", "content": "\n".join(lines)}],
    )
    raw = msg.content[0].text.strip()
    try:
        result = json.loads(raw)
        interesting = {k for k, v in result.items() if v}
    except (json.JSONDecodeError, AttributeError):
        # Fallback: show all if the model misbehaves
        interesting = set(df["sub_item"].unique())
    return interesting | ALWAYS_SHOW


# ---------------------------------------------------------------------------
# 3. Build chart
# ---------------------------------------------------------------------------

def build_chart(df: pd.DataFrame, show_sub_items: set[str]) -> bytes:
    """
    One subplot per interesting Q10 sub-item. Only plots wave > CHART_WAVE_MIN.
    Positive net balance = net tightening (adverse). Negative = net easing.
    Returns PNG bytes.
    """
    plot_df = df.copy()

    all_sub = df[["sub_item", "sub_item_label"]].drop_duplicates().sort_values("sub_item")
    visible = all_sub[all_sub["sub_item"].isin(show_sub_items)]

    n = len(visible)
    if n == 0:
        visible = all_sub[all_sub["sub_item"] == "a"]
        n = 1

    ncols = 2
    nrows = (n + 1) // ncols

    fig, axes = plt.subplots(nrows, ncols, figsize=(13, nrows * 3.8))
    # Normalise axes to a flat list regardless of shape
    if nrows == 1 and ncols == 1:
        axes_flat = [axes]
    elif nrows == 1 or ncols == 1:
        axes_flat = list(axes.flatten())
    else:
        axes_flat = list(axes.flatten())

    # Reserve space at top for the two-line suptitle
    fig.subplots_adjust(top=0.88, hspace=0.58, wspace=0.35)

    fig.suptitle(
        "Changes in Terms and Conditions of Bank Financing (Q10)\n"
        "Net balance: positive = net tightening (adverse for firms)  ·  negative = net easing",
        fontsize=11, fontweight="bold", color="#231f20",
    )

    waves = sorted(plot_df["wave_number"].unique())
    wave_labels = (
        plot_df[["wave_number", "survey_period_label"]]
        .drop_duplicates()
        .sort_values("wave_number")
        .set_index("wave_number")["survey_period_label"]
    )
    xtick_labels = [wave_labels[w] for w in waves]

    for ax, (_, row) in zip(axes_flat, visible.iterrows()):
        si = row["sub_item"]
        label = row["sub_item_label"]
        sub_df = plot_df[plot_df["sub_item"] == si]

        for country_code, country_name in COUNTRIES.items():
            cdf = sub_df[sub_df["country_code"] == country_code].sort_values("wave_number")
            if cdf.empty:
                continue
            ax.plot(
                cdf["wave_number"],
                cdf["net_balance_wtd"],
                label=country_name,
                color=COUNTRY_COLORS[country_code],
                linewidth=2,
                marker="o",
                markersize=4,
            )

        ax.axhline(0, color="#adadad", linewidth=0.8, linestyle="--")
        ax.set_title(label, fontsize=9, color="#231f20", pad=6)
        ax.set_xticks(waves)
        ax.set_xticklabels(xtick_labels, rotation=40, ha="right", fontsize=7)
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%+.0f"))
        ax.tick_params(axis="y", labelsize=8)
        ax.set_ylabel("Net balance (pp)", fontsize=7, color="#6a6a6a")
        ax.spines[["top", "right"]].set_visible(False)
        ax.set_facecolor("#f8f8f8")

    axes_flat[n - 1].legend(fontsize=8, loc="lower right", frameon=False, ncol=2)

    for ax in axes_flat[n:]:
        ax.set_visible(False)

    fig.patch.set_facecolor("#f8f8f8")

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# ---------------------------------------------------------------------------
# 4. Interpret with Claude
# ---------------------------------------------------------------------------

BULLET_SYSTEM = textwrap.dedent("""
    You are an ECB analyst writing concise bullets for a SAFE survey report.
    Write at most 3 bullet points about the latest wave's Q10 results.

    Language rules — follow these strictly:
    - Frame net balances as firm behaviour, not as a metric movement.
      CORRECT: "a net 26% of firms reported an increase in interest rates"
      WRONG:   "the net balance widened to 26pp" / "the net balance rose"
    - When comparing to prior wave: "compared with a net X% in the previous quarter"
    - Positive net balance = more firms reported tightening (adverse for firms).
      NEVER say "smoothing", "improving", or imply positive = good.
    - Negative net balance = more firms reported easing (favourable for firms).
    - Include sample size where meaningful: "a net 26% of firms (n=80) reported..."
    - Focus on the most notable change or country divergence across sub-items.
    - No headers, no preamble. Plain bullets starting with "•".
    - Keep each bullet to one sentence, max ~25 words.
""").strip()


def get_bullets(df: pd.DataFrame) -> list[str]:
    latest_wave = df["wave_number"].max()
    prev_wave = sorted(df["wave_number"].unique())[-2]

    latest = df[df["wave_number"] == latest_wave].copy()
    prev = df[df["wave_number"] == prev_wave].copy()

    def fmt(d: pd.DataFrame) -> str:
        rows = []
        for _, r in d.iterrows():
            rows.append(
                f"  {r['country_code']} | {r['sub_item_label']} | "
                f"net={r['net_balance_wtd']:+.1f}pp | "
                f"%tightened={r['pct_deteriorated_wtd']:.1f}% | "
                f"%eased={r['pct_improved_wtd']:.1f}% | "
                f"n={r['n_respondents']}"
            )
        return "\n".join(rows)

    user_msg = (
        f"Wave {latest_wave} (latest) results:\n{fmt(latest)}\n\n"
        f"Wave {prev_wave} (previous wave) results:\n{fmt(prev)}"
    )

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=200,
        system=BULLET_SYSTEM,
        messages=[{"role": "user", "content": user_msg}],
    )
    raw = msg.content[0].text.strip()
    bullets = [line.strip() for line in raw.splitlines() if line.strip().startswith("•")]
    return bullets[:3]


# ---------------------------------------------------------------------------
# 5. Build HTML report
# ---------------------------------------------------------------------------

HTML_TEMPLATE = textwrap.dedent("""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<style>
  body {{ font-family: Arial, sans-serif; background: #f8f8f8; color: #231f20;
          max-width: 900px; margin: 40px auto; padding: 0 24px; }}
  h1   {{ font-size: 20px; font-weight: bold; margin-bottom: 4px; }}
  .sub {{ color: #6a6a6a; font-size: 13px; margin-bottom: 28px; }}
  ul   {{ padding-left: 20px; }}
  li   {{ margin-bottom: 8px; font-size: 14px; line-height: 1.5; }}
  img  {{ width: 100%; margin-top: 24px; border: 1px solid #e0e0e0; }}
  .footer {{ color: #adadad; font-size: 11px; margin-top: 32px; }}
</style>
</head>
<body>
<h1>Changes in Terms and Conditions of Bank Financing (Q10)</h1>
<p class="sub">Slovakia · Euro Area · Austria · Germany &nbsp;|&nbsp; Generated {date}</p>
<ul>
{bullets}
</ul>
<img src="data:image/png;base64,{chart_b64}" alt="Q10 chart">
<p class="footer">Source: ECB SAFE microdata. Net balance = % reporting tightening minus % reporting easing.
Positive = net tightening (adverse for firms). Negative = net easing (favourable).</p>
</body>
</html>
""").strip()


def build_html(bullets: list[str], chart_png: bytes) -> str:
    bullet_html = "\n".join(f"  <li>{b.lstrip('• ').strip()}</li>" for b in bullets)
    chart_b64 = base64.b64encode(chart_png).decode()
    return HTML_TEMPLATE.format(
        date=date.today().strftime("%d %b %Y"),
        bullets=bullet_html,
        chart_b64=chart_b64,
    )


# ---------------------------------------------------------------------------
# 6. Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("Fetching Q10 data...")
    df = fetch_data()
    print(f"  {len(df)} rows, waves {df['wave_number'].min()}–{df['wave_number'].max()}")

    print("Checking which sub-items are interesting...")
    show_sub_items = check_interest(df)
    print(f"  Showing sub-items: {sorted(show_sub_items)}")

    print("Building chart...")
    chart_png = build_chart(df, show_sub_items)

    print("Generating bullets via Claude...")
    bullets = get_bullets(df)
    for b in bullets:
        print(f"  {b}")

    print("Assembling HTML...")
    html = build_html(bullets, chart_png)

    out_path = OUTPUT_DIR / "report_q10.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"Saved → {out_path}")

    print("Done.")


if __name__ == "__main__":
    main()
