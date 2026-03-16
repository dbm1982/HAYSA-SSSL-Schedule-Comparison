# scripts/sssl_scraper.py

import asyncio
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright

# =========================
# CONFIG
# =========================

CONTESTS_URL = (
    "https://sssl.sportspilot.com/Scheduler/public/contests.aspx"
    "?programid=1083&header=on&smode=&sportid="
)

REPORT_BASE_URL = (
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx"
    "?contest={cid}&header=on&print=1"
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
# HELPERS
# =========================

def load_location_mapping():
    if not MAPPING_PATH.exists():
        print(f"No mapping file found at: {MAPPING_PATH}")
        return None

    print(f"Using mapping file: {MAPPING_PATH}")
    df = pd.read_excel(MAPPING_PATH)
    df = df.rename(
        columns={
            df.columns[0]: "Raw Location",
            df.columns[1]: "Mapped Location",
        }
    )
    return df


async def safe_goto(page, url):
    try:
        return await page.goto(url, wait_until="networkidle", timeout=45000)
    except:
        print(f"Retrying navigation to {url} with wait_until='load'...")
        return await page.goto(url, wait_until="load", timeout=45000)


async def get_contest_ids(page):
    print(f"Loading contests page: {CONTESTS_URL}")
    await safe_goto(page, CONTESTS_URL)
    await page.wait_for_timeout(1500)

    html = await page.content()
    ids = re.findall(r"contest=(\d+)", html)
    unique_ids = sorted(set(ids))

    print(f"Discovered {len(unique_ids)} contest IDs: {unique_ids}")
    return unique_ids


async def fetch_contest_table(page, contest_id):
    url = REPORT_BASE_URL.format(cid=contest_id)
    print(f"Loading contest: {url}")

    await safe_goto(page, url)
    await page.wait_for_timeout(1500)

    html = await page.content()
    tables = pd.read_html(html)

    if not tables:
        print(f"No tables found for contest {contest_id}")
        return None

    df = tables[0].copy()

    # Normalize columns if possible
    if len(df.columns) >= 5:
        df = df.iloc[:, :5]
        df.columns = ["Date", "Time", "Home", "Away", "Location"]
    else:
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

    df["Location Mapped"] = df["Mapped Location"].fillna(df["Location"])
    df.drop(columns=["Raw Location", "Mapped Location"], inplace=True, errors="ignore")
    return df


# =========================
# MAIN SCRAPER
# =========================

async def run_sssl_scraper():
    print(f"=== SSSL Schedule Scrape Started ({SEASON_LABEL}) ===")
    print(f"Output will be written to: {SAVE_PATH}")

    mapping_df = load_location_mapping()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )

        page = await browser.new_page(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )

        contest_ids = await get_contest_ids(page)
        if not contest_ids:
            print("No contest IDs found — aborting")
            return

        all_frames = []

        for cid in contest_ids:
            try:
                df = await fetch_contest_table(page, cid)
                if df is None or df.empty:
                    print(f"No data for contest {cid}")
                    continue

                df = apply_location_mapping(df, mapping_df)
                all_frames.append(df)

            except Exception as e:
                print(f"Error processing contest {cid}: {e}")

        await browser.close()

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
    asyncio.run(run_sssl_scraper())


if __name__ == "__main__":
    main()
