# scripts/sssl.scraper.py

import asyncio
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright

# -----------------------------
# 2026 Contest URLs
# -----------------------------
contest_links = [
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2266&header=on&print=1",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2268&header=on&print=1",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2265&header=on&print=1",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2260&header=on&print=1",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2256&header=on&print=1",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2257&header=on&print=1",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2250&header=on&print=1",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2252&header=on&print=1",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2248&header=on&print=1",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2254&header=on&print=1",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2235&header=on&print=1",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2238&header=on&print=1",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2241&header=on&print=1",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2242&header=on&print=1",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2244&header=on&print=1",
]

# -----------------------------
# Helpers
# -----------------------------
def normalize_location(raw_loc: str) -> str:
    """
    Normalize SSSL Location to match 'SSSL Field Name' in mapping file.
    - If it contains ' / ', take the part after the slash.
    - Otherwise, return as-is.
    """
    if not isinstance(raw_loc, str):
        return ""
    raw_loc = raw_loc.strip()
    if " / " in raw_loc:
        return raw_loc.split(" / ", 1)[1].strip()
    return raw_loc


def extract_town_abbr(team_name: str) -> str:
    """
    Extract town abbreviation from a team name.
    e.g. "ABG BPG.2 (Ziady)" -> "ABG"
    """
    if not isinstance(team_name, str) or not team_name.strip():
        return ""
    return team_name.split(" ", 1)[0].strip()


# Output columns (match Selenium version)
HEADERS = [
    "Event ID", "Date", "Time", "Location",
    "Schedule Name", "Visitor", "V", "Home", "H"
]

# XPaths (same structure as Selenium version)
SCHEDULE_CONTAINER_XPATH = (
    "/html/body/form/div[3]/table[3]/tbody/tr/td/div/div[2]"
)

ROW_XPATH = (
    "/html/body/form/div[3]/table[3]/tbody/tr/td/div/div[2]/table/"
    "tbody/tr/td[2]/table/tbody/tr/td/div/div[1]/table/tbody/tr"
)


# -----------------------------
# Playwright scraping
# -----------------------------
async def scrape_contest(page, contest_url: str):
    print(f"Loading contest: {contest_url}")
    await page.goto(contest_url, wait_until="networkidle")

    # Wait for the schedule container to exist
    await page.wait_for_selector(f"xpath={SCHEDULE_CONTAINER_XPATH}")

    # Get all rows in the real schedule table
    rows = await page.locator(f"xpath={ROW_XPATH}").all()

    contest_rows = []

    for row in rows:
        cells = await row.locator("td").all_text_contents()
        values = [c.strip() for c in cells]

        # There is an extra leading <td> (blank/checkbox), so we need at least 10 cells.
        if len(values) < 10:
            continue

        # Corrected mapping (shifted by +1 because values[0] is blank)
        cleaned = [
            values[1],  # Event ID
            values[2],  # Date
            values[3],  # Time
            values[5],  # Location (raw SSSL string)
            None,       # Schedule Name (placeholder)
            values[6],  # Visitor
            values[7],  # V
            values[8],  # Home
            values[9],  # H
        ]

        contest_rows.append(cleaned)

    if not contest_rows:
        print(f"No rows found for contest: {contest_url}")
        return None

    df = pd.DataFrame(contest_rows, columns=HEADERS)
    df.dropna(how="all", inplace=True)

    if df.empty:
        print(f"Empty DataFrame after cleaning for contest: {contest_url}")
        return None

    return df


async def run_sssl_scraper():
    repo_root = Path(__file__).resolve().parents[1]
    mapping_path = repo_root / "data" / "mapping" / "Location Mapping.xlsx"
    output_dir = repo_root / "data" / "sssl"
    output_dir.mkdir(parents=True, exist_ok=True)
    save_path = output_dir / "SSSL_Spring_2026_Schedule.xlsx"

    print(f"Using mapping file: {mapping_path}")
    print(f"Output will be written to: {save_path}")

    all_data = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1920, "height": 3000})

        try:
            for url in contest_links:
                df = await scrape_contest(page, url)
                if df is not None and not df.empty:
                    all_data.append(df)
                else:
                    print(f"No valid data for contest: {url}")

        finally:
            await browser.close()

    if not all_data:
        print("No schedule data found across all contests.")
        return

    # -----------------------------
    # Combine & Apply Location + Town Mapping
    # -----------------------------
    master_df = pd.concat(all_data, ignore_index=True)

    # Load mapping file
    mapping_df = pd.read_excel(mapping_path)

    # Build:
    #  - FIELD_MAP: SSSL Field Name -> TS Field Code
    #  - TOWN_MAP: Town Abbreviation -> Town Name
    FIELD_MAP = dict(zip(
        mapping_df["SSSL Field Name"],
        mapping_df["TS Field Code"]
    ))

    TOWN_MAP = dict(zip(
        mapping_df["Town Abbreviation"],
        mapping_df["Town Name"]
    ))

    # Normalize SSSL Location to match mapping file
    master_df["SSSL Field Name"] = master_df["Location"].apply(normalize_location)

    # Apply field mapping
    master_df["Schedule Name"] = master_df["SSSL Field Name"].map(FIELD_MAP)

    # Report missing mappings (using normalized SSSL Field Name)
    missing = master_df[master_df["Schedule Name"].isna()]["SSSL Field Name"].unique()
    if len(missing) > 0:
        print("\n⚠️ Missing TS codes for the following SSSL field names:")
        for m in missing:
            if isinstance(m, str) and m.strip():
                print(f"   - {m}")

    # -----------------------------
    # Build HAYSA Team List
    # -----------------------------
    visitor_hola = master_df["Visitor"][master_df["Visitor"].str.contains("HOLA", na=False)]
    home_hola = master_df["Home"][master_df["Home"].str.contains("HOLA", na=False)]
    hola_teams = pd.concat([visitor_hola, home_hola]).drop_duplicates().sort_values()

    hayasa_df = pd.DataFrame({"Team Name": hola_teams})

    # -----------------------------
    # Build Unique Town List (Other Towns)
    # -----------------------------
    visitor_other = master_df["Visitor"][~master_df["Visitor"].str.contains("HOLA", na=False)]
    home_other = master_df["Home"][~master_df["Home"].str.contains("HOLA", na=False)]
    other_teams = pd.concat([visitor_other, home_other]).drop_duplicates().sort_values()

    # Extract town abbreviation, then map to Town Name via TOWN_MAP
    town_abbrs = other_teams.apply(extract_town_abbr)
    town_names = town_abbrs.apply(lambda abbr: TOWN_MAP.get(abbr, abbr))

    # Unique, sorted list of town names
    town_names = town_names.drop_duplicates().sort_values()

    other_df = pd.DataFrame({"Town Name": town_names})

    # -----------------------------
    # Save Excel Output
    # -----------------------------
    with pd.ExcelWriter(save_path) as writer:
        # Main schedule with both raw Location and Schedule Name
        master_df.to_excel(writer, index=False, sheet_name="SSSL Schedule")
        hayasa_df.to_excel(writer, index=False, sheet_name="HAYSA Teams")
        other_df.to_excel(writer, index=False, sheet_name="Other Towns")

    print(f"\nSaved SSSL schedule to {save_path}")


def main():
    asyncio.run(run_sssl_scraper())


if __name__ == "__main__":
    main()
