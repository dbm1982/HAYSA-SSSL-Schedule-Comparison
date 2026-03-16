# scripts/haysa_scraper.py

import asyncio
import re
from datetime import datetime
from io import StringIO
from pathlib import Path

import pandas as pd
from playwright.async_api import async_playwright

# =========================
# CONFIG
# =========================

BASE_URL = "https://www.haysa.org/schedules"

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

# Output path inside repo
REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO_ROOT / "data" / "haysa"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
SAVE_PATH = OUTPUT_DIR / f"HAYSA_{SEASON_LABEL.replace(' ', '_')}_Schedule.xlsx"

# =========================
# TEAM / AGE GROUP LOGIC
# =========================

AGE_GROUP_REGEX = re.compile(
    r"(\d+/\d+(?:/\d+)*|Grade\s*\d+/\d+|U\d+|PG)",
    re.IGNORECASE
)

HAYSA_TEAM_REGEX = re.compile(
    r"(\d+/\d+(?:/\d+)*|Grade\s*\d+/\d+)\s+(Boys|Girls|Coed)\s+(.+)$",
    re.IGNORECASE
)

def extract_age_group(team_name):
    if not isinstance(team_name, str):
        return None
    match = AGE_GROUP_REGEX.search(team_name)
    return match.group(1) if match else None

def extract_gender(team_name):
    if not isinstance(team_name, str):
        return None
    match = re.search(r"\b(Boys|Girls|Coed)\b", team_name, re.IGNORECASE)
    return match.group(1).title() if match else None

def is_haysa_team(name):
    if not isinstance(name, str):
        return False
    stripped = name.strip()
    if re.search(r"\(.+\)$", stripped):
        return True
    if HAYSA_TEAM_REGEX.search(stripped):
        return True
    HAYSA_KEYWORDS = ["HOLA", "Holbrook", "HAYSA", "H-"]
    lower = stripped.lower()
    return any(k.lower() in lower for k in HAYSA_KEYWORDS)

def classify_team_type(team_name):
    age_group = extract_age_group(team_name)
    if not age_group:
        return "Unknown"
    if age_group.lower().startswith("u"):
        return "Rec"
    return "Travel"

# =========================
# SCRAPING FUNCTIONS
# =========================

async def get_schedule_links(page):
    print(f"Loading schedules page: {BASE_URL}")
    await page.goto(BASE_URL, wait_until="networkidle")

    await page.wait_for_selector('xpath=//*[@id="SchedulesPageLayout"]//a')

    links = await page.locator('xpath=//*[@id="SchedulesPageLayout"]//a').all()

    schedule_links = []
    for link in links:
        href = await link.get_attribute("href")
        text = (await link.text_content() or "").strip()

        if href and "/schedule/" in href.lower():
            # FIX: Convert relative → absolute URL
            if href.startswith("/"):
                href = "https://www.haysa.org" + href

            schedule_links.append({"url": href, "division": text})

    print(f"Found {len(schedule_links)} schedule links")
    return schedule_links


async def extract_schedule_table(page):
    await page.wait_for_selector(
        'xpath=//*[@id="ctl00_ContentPlaceHolder1_StandingsResultsControl_ScheduleGrid_ctl00"]'
    )

    table_html = await page.locator(
        'xpath=//*[@id="ctl00_ContentPlaceHolder1_StandingsResultsControl_ScheduleGrid_ctl00"]'
    ).evaluate("el => el.outerHTML")

    df = pd.read_html(StringIO(table_html))[0]
    df.columns = ["Date", "Time", "Home", "Away", "Location"]
    return df


def clean_schedule_df(df, division):
    valid_days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    df = df[df["Date"].astype(str).str[:3].isin(valid_days)]

    def extract_number(text):
        text = str(text).strip()
        match = re.search(r"(\d+)$", text)
        if match:
            return text[:text.rfind(match.group())].strip(), int(match.group())
        return text.strip(), None

    df["H"] = df["Home"].apply(lambda x: extract_number(x)[1])
    df["Home"] = df["Home"].apply(lambda x: extract_number(x)[0])
    df["A"] = df["Away"].apply(lambda x: extract_number(x)[1])
    df["Away"] = df["Away"].apply(lambda x: extract_number(x)[0])

    df["Home | Away"] = df["Location"].apply(
        lambda x: "Home" if str(x).startswith("H-") else "Away"
    )

    df["Home Type"] = df["Home"].apply(classify_team_type)
    df["Away Type"] = df["Away"].apply(classify_team_type)
    df["Division"] = division

    return df


async def extract_schedule_data(page, url, division):
    print(f"Loading schedule page: {url}")
    await page.goto(url, wait_until="networkidle")

    df = await extract_schedule_table(page)
    print(f"Schedule table detected for division '{division}'")

    df = clean_schedule_df(df, division)
    return df

# =========================
# HAYSA TEAM ASSIGNMENT
# =========================

def identify_haysa_team(row):
    home_is_haysa = is_haysa_team(row["Home"])
    away_is_haysa = is_haysa_team(row["Away"])

    if home_is_haysa and not away_is_haysa:
        return row["Home"]
    if away_is_haysa and not home_is_haysa:
        return row["Away"]
    if home_is_haysa and away_is_haysa:
        return row["Home"] if row["Home | Away"] == "Home" else row["Away"]
    return None

# =========================
# OUTPUT FUNCTIONS
# =========================

def build_team_dataframe(all_teams):
    teams = pd.Series(all_teams).drop_duplicates().sort_values()
    df = pd.DataFrame(teams, columns=["Team"])
    df["Is HAYSA"] = df["Team"].apply(is_haysa_team)
    df["Age Group"] = df["Team"].apply(extract_age_group)
    df["Gender"] = df["Team"].apply(extract_gender)
    return df


def annotate_schedule_with_haysa(combined_schedule):
    combined_schedule["HAYSA Team"] = combined_schedule.apply(identify_haysa_team, axis=1)
    combined_schedule["Last Updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return combined_schedule


def build_games_per_team(combined_schedule, haysa_only):
    df = combined_schedule.copy()
    df = df[df["HAYSA Team"].notna()]

    counts = (
        df.groupby("HAYSA Team")
        .size()
        .reset_index(name="Games")
        .sort_values("Games", ascending=False)
    )

    counts = counts.merge(
        haysa_only[["Team", "Age Group", "Gender"]],
        left_on="HAYSA Team",
        right_on="Team",
        how="left"
    ).drop(columns=["Team"])

    return counts[["HAYSA Team", "Age Group", "Gender", "Games"]]


def build_games_by_team(combined_schedule, haysa_only):
    df = combined_schedule.copy()
    df = df[df["HAYSA Team"].notna()]

    df = df.merge(
        haysa_only[["Team", "Age Group", "Gender"]],
        left_on="HAYSA Team",
        right_on="Team",
        how="left"
    ).drop(columns=["Team"])

    df = df.sort_values(["HAYSA Team", "Date", "Time"])

    return df[
        [
            "HAYSA Team",
            "Age Group",
            "Gender",
            "Date",
            "Time",
            "Home",
            "Away",
            "Location",
            "Home | Away",
            "Division"
        ]
    ]


def save_outputs(combined_schedule, combined_teams, haysa_only, games_per_team, games_by_team):
    summary_df = pd.DataFrame({
        "Last Updated": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
        "Season": [SEASON_LABEL],
        "Total Games": [len(combined_schedule)],
        "Total Teams": [len(combined_teams)],
        "HAYSA Teams": [combined_teams["Is HAYSA"].sum()]
    })

    with pd.ExcelWriter(SAVE_PATH, engine="xlsxwriter") as writer:
        combined_schedule.to_excel(writer, sheet_name="HAYSA Schedule", index=False)
        combined_teams.to_excel(writer, sheet_name="All Teams", index=False)
        haysa_only.to_excel(writer, sheet_name="HAYSA Teams", index=False)
        games_per_team.to_excel(writer, sheet_name="Games per Team", index=False)
        games_by_team.to_excel(writer, sheet_name="Games by Team", index=False)
        summary_df.to_excel(writer, sheet_name="Summary", index=False)

    print(f"Excel file saved: {SAVE_PATH}")

# =========================
# MAIN
# =========================

async def run_haysa_scraper():
    print(f"=== HAYSA Schedule Scrape Started ({SEASON_LABEL}) ===")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        schedule_links = await get_schedule_links(page)
        if not schedule_links:
            print("No schedule links found — aborting")
            return

        all_schedules = []
        all_teams = []

        for link_info in schedule_links:
            df = await extract_schedule_data(page, link_info["url"], link_info["division"])
            if df is not None and not df.empty:
                all_schedules.append(df)
                all_teams.extend(df["Home"].tolist())
                all_teams.extend(df["Away"].tolist())
            else:
                print(f"No valid data for division '{link_info['division']}'")

        await browser.close()

    if not all_schedules:
        print("No valid schedules collected — nothing to save")
        return

    combined_schedule = pd.concat(all_schedules, ignore_index=True)
    combined_teams = build_team_dataframe(all_teams)

    haysa_only = combined_teams[combined_teams["Is HAYSA"] == True].copy()
    haysa_only = haysa_only[["Team", "Age Group", "Gender"]]
    haysa_only = haysa_only.sort_values(["Age Group", "Gender", "Team"])

    combined_schedule = annotate_schedule_with_haysa(combined_schedule)

    games_per_team = build_games_per_team(combined_schedule, haysa_only)
    games_by_team = build_games_by_team(combined_schedule, haysa_only)

    save_outputs(combined_schedule, combined_teams, haysa_only, games_per_team, games_by_team)

    print(f"=== HAYSA Schedule Scrape Completed ({SEASON_LABEL}) ===")


def main():
    asyncio.run(run_haysa_scraper())


if __name__ == "__main__":
    main()
