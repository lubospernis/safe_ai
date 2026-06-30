"""
SAFE Data Validation — compares mart net balances against ECB SDW published series.

Usage:
  python validation/validate.py              # prod (MotherDuck, needs MOTHERDUCK_TOKEN)
  python validation/validate.py --dev        # local dev.duckdb, no token needed
  python validation/validate.py --waves 4   # only validate last N waves (default 8)

Add checks to validation/checks.yml — one entry per series to validate.
Exits non-zero if any check fails, so it can be used as a CI gate.
"""

import argparse
import os
import sys
from pathlib import Path

import duckdb
import pandas as pd
import yaml
from ecbdata import ecbdata

CHECKS_FILE = Path(__file__).parent / "checks.yml"
DEV_DB      = Path(__file__).parent.parent / "dev.duckdb"
PROD_SCHEMA = "main_safe"
DEV_SCHEMA  = "main_safe_safe"


def fetch_ecb(series_key: str) -> pd.DataFrame:
    df = ecbdata.get_series(series_key)
    df = df[["TIME_PERIOD", "OBS_VALUE"]].copy()
    # Normalise ECB period format '2024-Q1' → '2024Q1' to match survey_period_label
    df["period"] = df["TIME_PERIOD"].str.replace("-", "", regex=False)
    df["ecb_value"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
    return df[["period", "ecb_value"]].dropna(subset=["ecb_value"])


def fetch_mart(check: dict, con: duckdb.DuckDBPyConnection, schema: str) -> pd.DataFrame:
    m = check["mart"]
    table = m["table"].replace(PROD_SCHEMA, schema)
    where = " AND ".join(f"{k} = '{v}'" for k, v in m["filters"].items())
    sql = f"""
        SELECT survey_period_label AS period, {m['value_col']} AS mart_value
        FROM {table}
        WHERE {where}
        ORDER BY period
    """
    return con.execute(sql).df()


def run_check(check: dict, con: duckdb.DuckDBPyConnection, schema: str, waves: int) -> dict:
    ecb_df  = fetch_ecb(check["ecb"]["series_key"])
    mart_df = fetch_mart(check, con, schema)

    merged = mart_df.merge(ecb_df, on="period", how="left")

    # Only score waves where ECB has published (inner on ecb_value)
    merged = merged.dropna(subset=["ecb_value"])

    # Warn if the mart has waves with no ECB counterpart yet
    missing_in_ecb = mart_df[~mart_df["period"].isin(ecb_df["period"])]
    if not missing_in_ecb.empty:
        periods = ", ".join(missing_in_ecb["period"].tolist())
        print(f"  [{check['id']}] WARNING: mart has periods not yet in ECB SDW: {periods}")

    merged = merged.tail(waves).copy()
    tol = check.get("tolerance_pp", 1.5)
    merged["diff"] = (merged["mart_value"] - merged["ecb_value"]).abs()
    merged["pass"] = merged["diff"] <= tol

    return {
        "id":           check["id"],
        "description":  check["description"],
        "rows_checked": len(merged),
        "max_diff":     merged["diff"].max() if len(merged) else float("nan"),
        "all_pass":     bool(merged["pass"].all()) if len(merged) else True,
        "detail":       merged,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate SAFE mart values against ECB SDW")
    parser.add_argument("--dev",   action="store_true", help="Use local dev.duckdb")
    parser.add_argument("--waves", type=int, default=8, help="Latest N waves to validate (default 8)")
    args = parser.parse_args()

    checks = yaml.safe_load(CHECKS_FILE.read_text())["checks"]
    schema = DEV_SCHEMA if args.dev else PROD_SCHEMA

    if args.dev:
        con = duckdb.connect(str(DEV_DB))
        print(f"[DEV] {DEV_DB}")
    else:
        token = os.environ["MOTHERDUCK_TOKEN"]
        con = duckdb.connect(f"md:my_db?motherduck_token={token}")
        print("[PROD] MotherDuck")

    print(f"Running {len(checks)} check(s), last {args.waves} waves each...\n")

    results = []
    for check in checks:
        try:
            r = run_check(check, con, schema, args.waves)
        except Exception as e:
            r = {
                "id": check["id"], "description": check["description"],
                "rows_checked": 0, "max_diff": float("nan"),
                "all_pass": False, "detail": pd.DataFrame(),
                "error": str(e),
            }
        results.append(r)

    con.close()

    # Summary table
    print(f"{'ID':<40} {'Rows':>5} {'MaxDiff':>9}  Status")
    print("─" * 62)
    any_fail = False
    for r in results:
        if "error" in r:
            print(f"{r['id']:<40} {'ERR':>5} {'—':>9}  FAIL  ({r['error']})")
            any_fail = True
            continue
        status = "PASS" if r["all_pass"] else "FAIL"
        if not r["all_pass"]:
            any_fail = True
        max_diff_str = f"{r['max_diff']:.2f}pp" if r["rows_checked"] else "—"
        print(f"{r['id']:<40} {r['rows_checked']:>5} {max_diff_str:>9}  {status}")
        if not r["all_pass"]:
            fails = r["detail"][~r["detail"]["pass"]]
            for _, row in fails.iterrows():
                print(f"    {row['period']}: mart={row['mart_value']:+.1f}  "
                      f"ecb={row['ecb_value']:+.1f}  diff={row['diff']:.1f}pp")

    print()
    if any_fail:
        print("RESULT: FAILED — one or more checks exceeded tolerance.")
        sys.exit(1)
    else:
        print("RESULT: ALL CHECKS PASSED")
        sys.exit(0)


if __name__ == "__main__":
    main()
