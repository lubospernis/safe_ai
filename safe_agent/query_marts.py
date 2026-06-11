"""Query MotherDuck mart tables and return structured data for Slovakia.

The agent reads two tiers of data:
  1. mart_safe__slovakia_kpis — one row per wave, pre-selected headline KPIs.
     The LLM uses these for stat cards and bullets. All series choices are made
     in the mart; the agent must NOT recompute values.
  2. Detailed mart tables — used only for time-series chart data (render_report.py).
     The LLM receives these as supporting context but should not derive new KPIs
     from them.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import duckdb
import pandas as pd


def _connect() -> duckdb.DuckDBPyConnection:
    token = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
    if not token:
        raise RuntimeError("Set MOTHERDUCK_TOKEN env var before running the agent.")
    os.environ.setdefault("motherduck_token", token)
    return duckdb.connect("md:my_db")


@dataclass
class ReportData:
    kpis: pd.DataFrame            # mart_safe__slovakia_kpis — headline numbers
    financing: pd.DataFrame       # mart_safe__financing_conditions — chart data
    business: pd.DataFrame        # mart_safe__business_situation — chart data
    pressingness: pd.DataFrame    # mart_safe__q0b_pressingness — chart data (deduped)
    loan_applications: pd.DataFrame
    outlook: pd.DataFrame
    expectations: pd.DataFrame
    latest_wave: int
    latest_period_label: str


def fetch_report_data(country_code: str = "SK", n_waves: int = 10) -> ReportData:
    con = _connect()

    latest_wave = con.execute(
        "SELECT MAX(wave_number) FROM main_safe.mart_safe__slovakia_kpis"
    ).fetchone()[0]
    wave_min = latest_wave - n_waves + 1

    # ── Tier 1: pre-selected KPIs (one row per wave, analyst-chosen series) ──
    kpis = con.execute(
        """
        SELECT * FROM main_safe.mart_safe__slovakia_kpis
        WHERE wave_number >= ?
        ORDER BY wave_number
        """,
        [wave_min],
    ).df()

    # ── Tier 2: detailed tables for chart rendering ──────────────────────────
    financing = con.execute(
        """
        SELECT * FROM main_safe.mart_safe__financing_conditions
        WHERE country_code = ? AND wave_number >= ?
        ORDER BY wave_number, question_id, sub_item
        """,
        [country_code, wave_min],
    ).df()

    business = con.execute(
        """
        SELECT * FROM main_safe.mart_safe__business_situation
        WHERE country_code = ? AND wave_number >= ?
        ORDER BY wave_number, sub_item
        """,
        [country_code, wave_min],
    ).df()

    # Deduplication in SQL: prefer 6m reference_period over 3m
    pressingness = con.execute(
        """
        SELECT * EXCLUDE (rn) FROM (
            SELECT *,
                row_number() OVER (
                    PARTITION BY wave_number, problem_id
                    ORDER BY CASE WHEN reference_period = '6m' THEN 1 ELSE 2 END
                ) AS rn
            FROM main_safe.mart_safe__q0b_pressingness
            WHERE country_code = ? AND wave_number >= ?
        ) t
        WHERE rn = 1
        ORDER BY wave_number, problem_id
        """,
        [country_code, wave_min],
    ).df()

    loan_applications = con.execute(
        """
        SELECT * FROM main_safe.mart_safe__loan_applications
        WHERE country_code = ? AND wave_number >= ?
        ORDER BY wave_number, sub_item
        """,
        [country_code, wave_min],
    ).df()

    outlook = con.execute(
        """
        SELECT * FROM main_safe.mart_safe__outlook
        WHERE country_code = ? AND wave_number >= ?
        ORDER BY wave_number, sub_item
        """,
        [country_code, wave_min],
    ).df()

    expectations = con.execute(
        """
        SELECT * FROM main_safe.mart_safe__expectations
        WHERE country_code = ? AND wave_number >= ?
        ORDER BY wave_number, question_id, sub_item
        """,
        [country_code, wave_min],
    ).df()

    period_label = (
        kpis.loc[kpis.wave_number == latest_wave, "survey_period_label"].iloc[0]
        if not kpis.empty
        else str(latest_wave)
    )

    con.close()

    return ReportData(
        kpis=kpis,
        financing=financing,
        business=business,
        pressingness=pressingness,
        loan_applications=loan_applications,
        outlook=outlook,
        expectations=expectations,
        latest_wave=latest_wave,
        latest_period_label=period_label,
    )


def to_json_payload(data: ReportData) -> dict:
    return {
        "latest_wave": data.latest_wave,
        "latest_period_label": data.latest_period_label,
        # Tier 1: agent uses these for all stat cards / bullets
        "kpis": data.kpis.to_dict(orient="records"),
        # Tier 2: chart data only — agent must not re-derive KPIs from these
        "financing_conditions": data.financing.to_dict(orient="records"),
        "business_situation": data.business.to_dict(orient="records"),
        "pressing_problems": data.pressingness.to_dict(orient="records"),
        "loan_applications": data.loan_applications.to_dict(orient="records"),
        "outlook": data.outlook.to_dict(orient="records"),
        "expectations": data.expectations.to_dict(orient="records"),
    }
