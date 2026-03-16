# scripts/sssl_scraper.py

import re
import requests
from datetime import datetime
from pathlib import Path

import pandas as pd

# =========================
# CONFIG
# =========================

CONTESTS_URL = (
    "https://sssl.sportspilot.com/Scheduler/public/contests.aspx"
    "?programid=1083&header=on&smode=&sportid="
)

REPORT_BASE_URL = (
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest={cid}&header=on&print=1"
)

def detect_season_label():
    now = datetime.now()
    year = now.year
    month = now.month
    if month in (3, 4, 5, 6):
        return f"Spring {year}"
    elif month in (8, 9, 10, 11):
        return f"Fall {year}"
    return f"{year}"

SEASON_LABEL = detect_season_label()

REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO_ROOT / "data" / "sssl"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
SAVE_PATH = OUTPUT_DIR / f"SSSL_{SEASON_LABEL.replace(' ', '_')}_Schedule.xlsx"

MAPPING_PATH = REPO_ROOT / "data" / "mapping" / "Location Mapping.xlsx"

# =========================
# HELPER FUNCTIONS
# =========================

def load_location_mapping():
    if not MAPPING_PATH.exists():
        print(f"No mapping file found at: {MAPPING_PATH} (continuing without mapping)")
        return None
    print(f"Using mapping file: {MAPPING_PATH}")
    mapping_df = pd.read_excel(MAPPING_PATH)
    # Expect columns like: "Raw Location", "Mapped Location"
    # Adjust if your actual column names differ.
    mapping_df = mapping_df.rename(
        columns={
            mapping_df.columns[0]: "Raw Location",
            mapping_df.columns[1]: "Mapped Location",
        }
    )
    return mapping_df


def get_contest_ids():
    print(f"Loading contests page: {CONTESTS_URL}")
    resp = requests.get(CONTESTS_URL, timeout=30)
    resp.raise_for_status()

    html = resp.text
    # Find all contest=#### patterns
    ids = re.findall(r"contest=(\d+)", html)
    unique_ids = sorted(set(ids))
    print(f"Discovered {len(unique_ids)} contest IDs: {unique_ids}")
    return unique_ids


def fetch_contest_table(contest_id):
    url = REPORT_BASE_URL.format(cid=contest_id)
    print(f"Loading contest: {url}")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    # Parse all tables on the page
    tables = pd.read_html(resp.text)
    if not tables:
        print(f"No tables found for contest {contest_id}")
        return None

    df = tables[0].copy()
    # Try to normalize columns if needed
    # Adjust this block if your table structure differs.
    if len(df.columns) >= 5:
        df = df.iloc[:, :5]
        df.columns = ["Date", "Time", "Home", "Away", "Location"]
    else:
        # Fallback: keep whatever columns exist
        df.columns = [str(c) for c in df.columns]

    df["Contest ID"] = contest_id
    return df


def apply_location_mapping(df, mapping_df):
    if mapping_df is None or "Location" not in df.columns:
        return df

    df = df.merge(
        mapping_df,
        left_on="Location",
        right_on="Raw Location",
        how="left",
    )
    # If mapping exists, use it; otherwise keep original
    df["Location Mapped"] = df["Mapped Location"].fillna(df["Location"])
    df.drop(columns=["Raw Location", "Mapped Location"], inplace=True, errors="ignore")
    return df


# =========================
# MAIN SCRAPE LOGIC
# =========================

def run_sssl_scraper():
    print(f"=== SSSL Schedule Scrape Started ({SEASON_LABEL}) ===")
    print(f"Output will be written to: {SAVE_PATH}")

    mapping_df = load_location_mapping()
    contest_ids = get_contest_ids()
    if not contest_ids:
        print("No contest IDs discovered — aborting")
        return

    all_frames = []

    for cid in contest_ids:
        try:
            df = fetch_contest_table(cid)
            if df is None or df.empty:
                print(f"No data for contest {cid}")
                continue

            df = apply_location_mapping(df, mapping_df)
            all_frames.append(df)
        except Exception as e:
            print(f"Error processing contest {cid}: {e}")

    if not all_frames:
        print("No valid contest data collected — nothing to save")
        return

    combined = pd.concat(all_frames, ignore_index=True)
    combined["Season"] = SEASON_LABEL
    combined["Last Updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with pd.ExcelWriter(SAVE_PATH, engine="xlsxwriter") as writer:
        combined.to_excel(writer, sheet_name="SSSL Schedule", index=False)

    print(f"Excel file saved: {SAVE_PATH}")
    print(f"=== SSSL Schedule Scrape Completed ({SEASON_LABEL}) ===")


def main():
    run_sssl_scraper()


if __name__ == "__main__":
    main()
