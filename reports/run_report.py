"""
SAFE Survey Report Generator
Runs Q10 SQL, generates chart, calls Claude for bullets, writes HTML, sends email.

Required environment variables:
  MOTHERDUCK_TOKEN   — MotherDuck service token
  ANTHROPIC_API_KEY  — Anthropic API key
  SMTP_HOST          — e.g. smtp.gmail.com
  SMTP_PORT          — e.g. 587
  SMTP_USER          — sender email address
  SMTP_PASSWORD      — sender email password / app password
  REPORT_TO_EMAIL    — recipient email address (comma-separated for multiple)
"""

import base64
import io
import os
import smtplib
import textwrap
from datetime import date
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
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

REPO_ROOT = Path(__file__).parent.parent
SQL_DIR = Path(__file__).parent / "sql"
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

COUNTRIES = {"SK": "Slovakia", "EA": "Euro Area", "AT": "Austria", "DE": "Germany"}
COUNTRY_COLORS = {"SK": "#bd4e35", "EA": "#0777b3", "AT": "#2d7a00", "DE": "#e18727"}

MOTHERDUCK_TOKEN = os.environ["MOTHERDUCK_TOKEN"]
ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ["SMTP_USER"]
SMTP_PASSWORD = os.environ["SMTP_PASSWORD"]
REPORT_TO = [e.strip() for e in os.environ["REPORT_TO_EMAIL"].split(",")]


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
# 2. Build chart
# ---------------------------------------------------------------------------

def build_chart(df: pd.DataFrame) -> bytes:
    """
    One subplot per Q10 sub-item. Lines show net_balance_wtd per country.
    Positive = net tightening (adverse). Negative = net easing (favourable).
    Returns PNG bytes.
    """
    sub_items = df[["sub_item", "sub_item_label"]].drop_duplicates().sort_values("sub_item")
    n = len(sub_items)
    ncols = 2
    nrows = (n + 1) // ncols

    fig, axes = plt.subplots(nrows, ncols, figsize=(13, nrows * 3.5), constrained_layout=True)
    axes = axes.flatten()

    fig.suptitle(
        "Changes in Terms and Conditions of Bank Financing (Q10)\n"
        "Net balance: positive = tightening  ·  negative = easing",
        fontsize=12, fontweight="bold", color="#231f20", y=1.01,
    )

    waves = sorted(df["wave_number"].unique())
    wave_labels = (
        df[["wave_number", "survey_period_label"]]
        .drop_duplicates()
        .sort_values("wave_number")
        .set_index("wave_number")["survey_period_label"]
    )
    xtick_labels = [wave_labels[w] for w in waves]

    for ax, (_, row) in zip(axes, sub_items.iterrows()):
        si = row["sub_item"]
        label = row["sub_item_label"]
        sub_df = df[df["sub_item"] == si]

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
        ax.set_xticklabels(xtick_labels, rotation=45, ha="right", fontsize=7)
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%+.0f"))
        ax.tick_params(axis="y", labelsize=8)
        ax.set_ylabel("Net balance (pp)", fontsize=7, color="#6a6a6a")
        ax.spines[["top", "right"]].set_visible(False)
        ax.set_facecolor("#f8f8f8")

    # Legend on last used axis; hide any unused axes
    used = len(sub_items)
    axes[used - 1].legend(
        fontsize=8, loc="lower right", frameon=False,
        ncol=2,
    )
    for ax in axes[used:]:
        ax.set_visible(False)

    fig.patch.set_facecolor("#f8f8f8")

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


# ---------------------------------------------------------------------------
# 3. Interpret with Claude
# ---------------------------------------------------------------------------

BULLET_SYSTEM = textwrap.dedent("""
    You are an ECB analyst writing bullet points for a quarterly SAFE survey report.
    Write at most 3 bullet points about the latest wave's Q10 results.

    Style rules — follow these strictly:
    - Cite % figures AND sample size: "30% of firms (n=80) reported..."
    - Compare to the previous wave where meaningful.
    - Positive net balance = net tightening (adverse for firms).
    - Negative net balance = net easing (favourable for firms).
    - Focus on the most notable changes or divergences between countries.
    - Keep each bullet to one sentence. No headers, no preamble.
    - Output plain bullet points starting with "•".
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
        max_tokens=400,
        system=BULLET_SYSTEM,
        messages=[{"role": "user", "content": user_msg}],
    )
    raw = msg.content[0].text.strip()
    bullets = [line.strip() for line in raw.splitlines() if line.strip().startswith("•")]
    return bullets[:3]


# ---------------------------------------------------------------------------
# 4. Build HTML report
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
<p class="footer">Source: ECB SAFE microdata. Net balance = % reporting increase minus % reporting decrease.
Positive = tightening (adverse for firms). Negative = easing (favourable).</p>
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
# 5. Send email
# ---------------------------------------------------------------------------

def send_email(subject: str, html_body: str, chart_png: bytes) -> None:
    msg = MIMEMultipart("related")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(REPORT_TO)

    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(html_body, "html"))
    msg.attach(alt)

    img = MIMEImage(chart_png, name="q10_chart.png")
    img.add_header("Content-Disposition", "attachment", filename="q10_chart.png")
    msg.attach(img)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, REPORT_TO, msg.as_string())

    print(f"Email sent to {', '.join(REPORT_TO)}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("Fetching Q10 data...")
    df = fetch_data()
    print(f"  {len(df)} rows, waves {df['wave_number'].min()}–{df['wave_number'].max()}")

    print("Building chart...")
    chart_png = build_chart(df)

    print("Generating bullets via Claude...")
    bullets = get_bullets(df)
    for b in bullets:
        print(f"  {b}")

    print("Assembling HTML...")
    html = build_html(bullets, chart_png)

    out_path = OUTPUT_DIR / "report_q10.html"
    out_path.write_text(html, encoding="utf-8")
    print(f"Saved → {out_path}")

    print("Sending email...")
    latest_wave = df["wave_number"].max()
    label = df[df["wave_number"] == latest_wave]["survey_period_label"].iloc[0]
    send_email(
        subject=f"SAFE Survey Q10 Report — {label}",
        html_body=html,
        chart_png=chart_png,
    )

    print("Done.")


if __name__ == "__main__":
    main()
