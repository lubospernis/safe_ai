"""Query MotherDuck mart tables and return structured data for Slovakia."""

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
    financing: pd.DataFrame
    business: pd.DataFrame
    pressingness: pd.DataFrame
    loan_applications: pd.DataFrame
    outlook: pd.DataFrame
    expectations: pd.DataFrame
    latest_wave: int
    latest_period_label: str


def fetch_report_data(country_code: str = "SK", n_waves: int = 10) -> ReportData:
    con = _connect()

    latest_wave = con.execute(
        "SELECT MAX(wave_number) FROM main_safe.mart_safe__financing_conditions"
    ).fetchone()[0]
    wave_min = latest_wave - n_waves + 1

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

    # Deduplicate pressingness: from wave 30 onward both 6m and 3m rows exist.
    # Keep 6m as the primary reference period; fall back to 3m where 6m is absent.
    pressingness_raw = con.execute(
        """
        SELECT * FROM main_safe.mart_safe__q0b_pressingness
        WHERE country_code = ? AND wave_number >= ?
        ORDER BY wave_number, problem_id, reference_period
        """,
        [country_code, wave_min],
    ).df()

    if not pressingness_raw.empty and "reference_period" in pressingness_raw.columns:
        # Rank: 6m = 1 (preferred), 3m = 2
        pressingness_raw["_rp_rank"] = pressingness_raw["reference_period"].map(
            lambda x: 1 if x == "6m" else 2
        )
        pressingness = (
            pressingness_raw.sort_values("_rp_rank")
            .drop_duplicates(subset=["wave_number", "problem_id"])
            .drop(columns=["_rp_rank"])
            .reset_index(drop=True)
        )
    else:
        pressingness = pressingness_raw

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
        financing.loc[financing.wave_number == latest_wave, "survey_period_label"].iloc[0]
        if not financing.empty
        else str(latest_wave)
    )

    con.close()

    return ReportData(
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
        "financing_conditions": data.financing.to_dict(orient="records"),
        "business_situation": data.business.to_dict(orient="records"),
        "pressing_problems": data.pressingness.to_dict(orient="records"),
        "loan_applications": data.loan_applications.to_dict(orient="records"),
        "outlook": data.outlook.to_dict(orient="records"),
        "expectations": data.expectations.to_dict(orient="records"),
    }
