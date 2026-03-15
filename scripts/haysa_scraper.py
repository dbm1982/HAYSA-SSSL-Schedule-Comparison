import os
import re
import time
from io import StringIO
from datetime import datetime
import traceback

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from tqdm import tqdm

# =========================
# CONFIG
# =========================

DRY_RUN = False
EXPORT_CSV = False

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

BASE_DIR = "data/haysa"
os.makedirs(BASE_DIR, exist_ok=True)
SAVE_PATH_XLSX = os.path.join(BASE_DIR, f"Full HAYSA {SEASON_LABEL} Schedule.xlsx")
SAVE_PATH_CSV = os.path.join(BASE_DIR, f"Full HAYSA {SEASON_LABEL} Schedule.csv")

LOG_DIR = r"C:\Users\dbm19\OneDrive\Documents\HAYSA\Scripts\Logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_PATH = os.path.join(LOG_DIR, f"HAYSA_status_log_{SEASON_LABEL.replace(' ', '_')}.txt")

# =========================
# LOGGING
# =========================

def log_status(message, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] [{level}] {message}"
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line)

def log_exception(context, exc):
    tb = traceback.format_exc()
    log_status(f"{context}: {exc}", level="ERROR")
    log_status(tb, level="DEBUG")

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
# SELENIUM DRIVER
# =========================

def setup_driver():
    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        driver = webdriver.Chrome(options=chrome_options)
        log_status("WebDriver initialized")
        return driver
    except Exception as e:
        log_exception("Failed to initialize WebDriver", e)
        return None

# =========================
# SCRAPING FUNCTIONS
# =========================

def get_schedule_links(driver, base_url):
    try:
        log_status(f"Loading schedules page: {base_url}")
        driver.get(base_url)

        WebDriverWait(driver, 15).until(
            EC.presence_of_all_elements_located((By.XPATH, '//*[@id="SchedulesPageLayout"]//a'))
        )

        links = driver.find_elements(By.XPATH, '//*[@id="SchedulesPageLayout"]//a')
        schedule_links = []
        for link in links:
            href = link.get_attribute("href")
            text = link.text.strip()
            if href and "/schedule/" in href.lower():
                schedule_links.append({"url": href, "division": text})

        log_status(f"Found {len(schedule_links)} schedule links")
        return schedule_links

    except Exception as e:
        log_exception("Error extracting schedule links", e)
        return []

def extract_schedule_table(driver):
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located(
            (By.XPATH, "//*[@id='ctl00_ContentPlaceHolder1_StandingsResultsControl_ScheduleGrid_ctl00']")
        )
    )
    table_element = driver.find_element(
        By.XPATH, "//*[@id='ctl00_ContentPlaceHolder1_StandingsResultsControl_ScheduleGrid_ctl00']"
    )
    table_html = driver.execute_script("return arguments[0].outerHTML;", table_element)
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

def extract_schedule_data(url, driver, division):
    try:
        log_status(f"Loading schedule page: {url}")
        driver.get(url)
        time.sleep(2)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

        df = extract_schedule_table(driver)
        log_status(f"Schedule table detected for division '{division}'")
        df = clean_schedule_df(df, division)
        return df

    except Exception as e:
        log_exception(f"Error extracting data from {url}", e)
        return None

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

    if DRY_RUN:
        log_status("Dry run enabled — no files will be written", level="WARN")
        return

    with pd.ExcelWriter(SAVE_PATH_XLSX, engine="xlsxwriter") as writer:
        combined_schedule.to_excel(writer, sheet_name="Schedule", index=False)
        combined_teams.to_excel(writer, sheet_name="All Teams", index=False)
        haysa_only.to_excel(writer, sheet_name="HAYSA Teams", index=False)
        games_per_team.to_excel(writer, sheet_name="Games per Team", index=False)
        games_by_team.to_excel(writer, sheet_name="Games by Team", index=False)
        summary_df.to_excel(writer, sheet_name="Summary", index=False)

    log_status(f"Excel file saved: {SAVE_PATH_XLSX}")

    if EXPORT_CSV:
        combined_schedule.to_csv(SAVE_PATH_CSV, index=False, encoding="utf-8-sig")
        log_status(f"CSV file saved: {SAVE_PATH_CSV}")

# =========================
# MAIN
# =========================

def main():
    log_status(f"=== HAYSA Schedule Scrape Started ({SEASON_LABEL}) ===")

    driver = setup_driver()
    if driver is None:
        log_status("Aborting: WebDriver failed to initialize", level="ERROR")
        return

    try:
        schedule_links = get_schedule_links(driver, BASE_URL)
        if not schedule_links:
            log_status("No schedule links found — aborting", level="ERROR")
            return

        all_schedules = []
        all_teams = []

        for link_info in tqdm(schedule_links, desc="Processing schedules", unit="division", dynamic_ncols=True):
            df = extract_schedule_data(link_info["url"], driver, link_info["division"])
            if df is not None and not df.empty:
                all_schedules.append(df)
                all_teams.extend(df["Home"].tolist())
                all_teams.extend(df["Away"].tolist())
            else:
                log_status(f"No valid data for division '{link_info['division']}'", level="WARN")

        if not all_schedules:
            log_status("No valid schedules collected — nothing to save", level="ERROR")
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

        log_status(f"=== HAYSA Schedule Scrape Completed ({SEASON_LABEL}) ===")

    except Exception as e:
        log_exception("Fatal error in main()", e)
    finally:
        try:
            driver.quit()
            log_status("WebDriver closed")
        except Exception:
            pass

if __name__ == "__main__":
    main()
