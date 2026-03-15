from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import pandas as pd
import re
from datetime import datetime

# -----------------------------
# Spring 2026 Contest Links
# -----------------------------
contest_links = [
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2266&header=on",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2268&header=on",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2265&header=on",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2260&header=on",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2256&header=on",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2257&header=on",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2250&header=on",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2252&header=on",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2248&header=on",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2254&header=on",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2235&header=on",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2238&header=on",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2241&header=on",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2242&header=on",
    "https://sssl.sportspilot.com/Scheduler/public/report.aspx?contest=2244&header=on",
]

HEADERS = ["Event ID", "Date", "Time", "End", "Location", "Visitor", "V", "Home", "H"]

# -----------------------------
# HOLA SSSL → TS mapping (case-insensitive match)
# -----------------------------
HOLA_SSSL_TO_TS = {
    "HOLA GPG.1 (Green)": "11/12/PG Girls Travel (Green)",
    "HOLA BPG1 (Caldwell)": "11/12/PG Boys (Caldwell)",
    "HOLA B4.5 (Ayers)": "3/4 Boys (Ayers)",
    "HOLA B4.2 (Walsh)": "3/4 Boys (Walsh)",
    "HOLA G4.5 (Lamb-williams)": "3/4 Girls (Lamb-Williams)",
    "HOLA G4.3 (Mills)": "3/4 Girls (Mills)",
    "HOLA G4.2 (Picardi)": "3/4 Girls (Picardi)",
    "HOLA B6.2 (Casavant)": "5/6 Boys (Casavant)",
    "HOLA B6.4 (Johnson)": "5/6 Boys (Johnson)",
    "HOLA G6.4 (Baird-miller)": "5/6 Girls (Baird-Miller)",
    "HOLA G6.2 (Gracie)": "5/6 Girls (Gracie)",
    "HOLA B8.2 (Luyo)": "7/8 Boys (Luyo)",
    "HOLA B8.3 (Mills)": "7/8 Boys (Mills)",
    "HOLA G8.2 (Lucci-Mcshain)": "7/8 Girls (Lucci-McShain)",
    "HOLA2 B10.2 (Forbes)": "9/10 Boys (Forbes)",
    "HOLA1 B10.2 (Lauterhan)": "9/10 Boys TBD",
}

HOLA_LOOKUP = {k.lower(): v for k, v in HOLA_SSSL_TO_TS.items()}

# -----------------------------
# TS Name → Division Name mapping
# -----------------------------
HOLA_TS_TO_DIVISION = {
    "11/12/PG Girls Travel (Green)": "11/12/PG Girls",
    "11/12/PG Boys (Caldwell)": "11/12/PG Boys",
    "3/4 Boys (Ayers)": "Grade 3/4 Boys",
    "3/4 Boys (Walsh)": "Grade 3/4 Boys",
    "3/4 Girls (Lamb-Williams)": "Grade 3/4 Girls",
    "3/4 Girls (Mills)": "Grade 3/4 Girls",
    "3/4 Girls (Picardi)": "Grade 3/4 Girls",
    "5/6 Boys (Casavant)": "Grade 5/6 Boys",
    "5/6 Boys (Johnson)": "Grade 5/6 Boys",
    "5/6 Girls (Baird-Miller)": "Grade 5/6 Girls",
    "5/6 Girls (Gracie)": "Grade 5/6 Girls",
    "7/8 Boys (Luyo)": "Grade 7/8 Boys",
    "7/8 Boys (Mills)": "Grade 7/8 Boys",
    "7/8 Girls (Lucci-McShain)": "Grade 7/8 Girls",
    "9/10 Boys (Forbes)": "Grade 9/10 Boys",
    "9/10 Boys TBD": "Grade 9/10 Boys",
}

# -----------------------------
# Location Mapping
# -----------------------------
mapping_path = "data/mapping/Location Mapping.xlsx"
mapping_df = pd.read_excel(mapping_path)

def clean_location(loc):
    if not isinstance(loc, str):
        return loc

    loc = loc.replace("\xa0", " ")

    # Extract the town code from the prefix (e.g., "EAS /", "EBG /")
    m = re.match(r"^([A-Z]{3})\s*/", loc)
    town = m.group(1) if m else None

    # Remove patterns like "EAS / EAS -", "EBG / EBG -"
    loc = re.sub(r"^[A-Z]{3}\s*/\s*[A-Z]{3}\s*-\s*", "", loc)

    # Remove patterns like "EAS / EAS " (no dash)
    loc = re.sub(r"^[A-Z]{3}\s*/\s*[A-Z]{3}\s*", "", loc)

    # Normalize spaces
    loc = re.sub(r"\s+", " ", loc).strip()

    # Rebuild to match mapping file format
    if town:
        # If mapping file uses "TOWN - Field Name"
        if " - " in loc:
            return f"{town} - {loc}"
        # If mapping file uses "TOWN-Field Name"
        return f"{town}-{loc}"

    return loc

mapping_df["Clean SSSL"] = mapping_df["SSSL Field Name"].apply(clean_location)

FIELD_MAP = dict(zip(mapping_df["Clean SSSL"], mapping_df["TS Field Code"]))
TOWN_NAME_MAP = dict(zip(mapping_df["Town Abbreviation"], mapping_df["Town Name"]))

# -----------------------------
# Time Normalization
# -----------------------------
def normalize_time(t):
    if not isinstance(t, str):
        return t
    raw = t.strip()

    m = re.match(r"^(\d{1,2}):(\d{2})\s*([AP]M)$", raw, re.IGNORECASE)
    if m:
        hour = int(m.group(1))
        minute = m.group(2)
        ampm = m.group(3).upper()
        return f"{hour}:{minute} {ampm}"

    m = re.match(r"^(\d{1,2}):(\d{2})$", raw)
    if m:
        hour = int(m.group(1))
        minute = m.group(2)
        ampm = "PM" if hour == 12 else "AM"
        return f"{hour}:{minute} {ampm}"

    return raw

# -----------------------------
# Date Parsing for Sorting
# -----------------------------
def parse_date_for_sorting(date_str):
    if not isinstance(date_str, str):
        return None
    s = date_str.strip()

    for fmt in ["%m/%d/%Y", "%m/%d/%y"]:
        try:
            dt = datetime.strptime(s, fmt)
            if fmt == "%m/%d/%y" and dt.year < 2000:
                dt = dt.replace(year=dt.year + 2000)
            return dt
        except:
            pass
    return None

# -----------------------------
# Team parsing
# -----------------------------
def normalize_town(raw):
    if not isinstance(raw, str):
        return ""
    raw = raw.upper()
    raw = re.sub(r"[^A-Z/-]", "", raw)
    parts = re.split(r"[-/]", raw)
    return parts[0]

team_regex = re.compile(
    r"""
    ^\s*
    (?P<town>[A-Z][A-Z0-9/-]*)        
    \s+
    (?P<gender>[GB])                  
    (?P<age>(?:PG|\d+))               
    (?:\.?(?P<division>\d))?          
    (?:\s*\((?P<coach1>[^)]*)\))?     
    (?:\s+(?P<coach2>[A-Za-z][A-Za-z '.-]*))?
    \s*$
    """,
    re.VERBOSE
)

AGE_MAP = {
    "3": "U3/U4", "4": "3/4", "5": "5", "6": "5/6",
    "7": "7", "8": "7/8", "9": "9", "10": "9/10",
    "PG": "PG"
}

def parse_team(team):
    m = team_regex.match(team)
    if not m:
        return None

    gd = m.groupdict()
    town = normalize_town(gd["town"])
    if town.startswith("HOLA"):
        town = "HOLA"

    coach = gd["coach1"] or gd["coach2"] or ""
    coach = coach.strip()

    return {
        "Town": town,
        "Gender": gd["gender"],
        "AgeCode": gd["age"],
        "AgeGroup": AGE_MAP.get(gd["age"], gd["age"]),
        "Division": gd["division"],
        "Coach": coach
    }

def build_team_name(team_dict, raw_team_string):
    raw_lower = raw_team_string.lower()

    # HOLA mapping
    if raw_lower in HOLA_LOOKUP:
        return HOLA_LOOKUP[raw_lower]

    # Parsed team (rare for SSSL)
    if team_dict:
        town = team_dict["Town"]
        return TOWN_NAME_MAP.get(town, town).upper()

    # Fallback: use the raw SSSL team name
    return raw_team_string.upper()

def get_competitor_town(visitor_str, home_str):
    v = parse_team(visitor_str)
    h = parse_team(home_str)

    if v and v["Town"] == "HOLA" and h:
        return TOWN_NAME_MAP.get(h["Town"], h["Town"]).upper()

    if h and h["Town"] == "HOLA" and v:
        return TOWN_NAME_MAP.get(v["Town"], v["Town"]).upper()

    return ""

# -----------------------------
# Contest Label Extraction
# -----------------------------
def get_contest_label(page):
    try:
        text = page.inner_text("body")
    except:
        return "Unknown Contest"

    for line in text.splitlines():
        if line.strip().startswith("Contest:"):
            return line.replace("Contest:", "").strip()

    return "Unknown Contest"

# -----------------------------
# Scraper
# -----------------------------
game_line = re.compile(
    r"^(\d{7,8})\s+"
    r"(\d{1,2}/\d{1,2}/\d{4})\s+"
    r"(\d{1,2}:\d{2}\s+[AP]M)\s+"
    r"(\d{1,2}:\d{2}\s+[AP]M)\s+"
    r"(.+?)\s+"
    r"([A-Z0-9].+?)\s+"
    r"(NA|\d+)\s+"
    r"([A-Z0-9].+?)\s+"
    r"(NA|\d+)$"
)

def scrape_contest(page, url):
    try:
        page.goto(url + "&print=1", wait_until="load", timeout=60000)
    except PlaywrightTimeoutError:
        print(f"Timeout loading {url}, skipping.")
        return pd.DataFrame(columns=HEADERS)

    lines = [l.strip() for l in page.inner_text("body").splitlines() if l.strip()]
    rows = [list(m.groups()) for m in map(game_line.match, lines) if m]
    return pd.DataFrame(rows, columns=HEADERS)

# -----------------------------
# Extract HOLA-only fields
# -----------------------------
def extract_hola_fields(row):
    visitor = parse_team(row["Visitor"])
    home = parse_team(row["Home"])

    if visitor and visitor["Town"] == "HOLA":
        return {
            "Is_HOLA": True,
            "Home | Away": "Away",
            "HOLA_Team": row["Visitor"],
            "HOLA_Gender": visitor["Gender"],
            "HOLA_AgeCode": visitor["AgeCode"],
            "HOLA_AgeGroup": visitor["AgeGroup"],
            "HOLA_Division": visitor["Division"],
            "HOLA_Coach": visitor["Coach"],
            "Opponent": row["Home"]
        }

    if home and home["Town"] == "HOLA":
        return {
            "Is_HOLA": True,
            "Home | Away": "Home",
            "HOLA_Team": row["Home"],
            "HOLA_Gender": home["Gender"],
            "HOLA_AgeCode": home["AgeCode"],
            "HOLA_AgeGroup": home["AgeGroup"],
            "HOLA_Division": home["Division"],
            "HOLA_Coach": home["Coach"],
            "Opponent": row["Visitor"]
        }

    return {
        "Is_HOLA": False,
        "Home | Away": "",
        "HOLA_Team": "",
        "HOLA_Gender": "",
        "HOLA_AgeCode": "",
        "HOLA_AgeGroup": "",
        "HOLA_Division": "",
        "HOLA_Coach": "",
        "Opponent": ""
    }

# -----------------------------
# Division Name from HOLA TS name
# -----------------------------
def get_division_name(row):
    if not row["Is_HOLA"]:
        return ""

    hola_ts_name = (
        row["Home Team Name"]
        if row["Home | Away"] == "Home"
        else row["Away Team Name"]
    )

    return HOLA_TS_TO_DIVISION.get(hola_ts_name, "")

# -----------------------------
# Main
# -----------------------------
def main():
    all_frames = []
    contest_map = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for url in contest_links:
            contest_id = url.split("contest=")[1].split("&")[0]

            print(f"Scraping {url} ...")

            try:
                page.goto(url, wait_until="load", timeout=60000)
            except PlaywrightTimeoutError:
                print(f"  !! Timeout loading {url}")
                continue

            contest_label = get_contest_label(page)
            contest_map[contest_id] = contest_label

            print(f"  Contest: {contest_label}")

            df = scrape_contest(page, url)
            print(f"  -> {len(df)} games found")

            if not df.empty:
                all_frames.append(df)

        browser.close()

    print("\nContest ID → Contest Label")
    for cid, label in contest_map.items():
        print(f"{cid} → {label}")
    print()

    if not all_frames:
        print("No schedule data found.")
        return

    master_df = pd.concat(all_frames, ignore_index=True)
    master_df.drop_duplicates(subset=["Event ID"], inplace=True)

    hola_info = master_df.apply(extract_hola_fields, axis=1)
    final_df = pd.concat([master_df, hola_info.apply(pd.Series)], axis=1)

    # Clean location and map to TS field code
    final_df["CleanLocation"] = final_df["Location"].apply(clean_location)
    final_df["Schedule Name"] = final_df["CleanLocation"].map(FIELD_MAP).fillna("")

    # Normalize Time
    final_df["Time"] = final_df["Time"].apply(normalize_time)

    # Team names
    final_df["Away Team Name"] = final_df.apply(
        lambda row: build_team_name(parse_team(row["Visitor"]), row["Visitor"]),
        axis=1
    )

    final_df["Home Team Name"] = final_df.apply(
        lambda row: build_team_name(parse_team(row["Home"]), row["Home"]),
        axis=1
    )

    # Division Name from HOLA TS name
    final_df["Division Name"] = final_df.apply(get_division_name, axis=1)

    # Town Competitors (for HOLA games)
    final_df["Town Competitors"] = final_df.apply(
        lambda row: get_competitor_town(row["Visitor"], row["Home"]) if row["Is_HOLA"] else "",
        axis=1
    )

    # Sorting Logic
    final_df["SortDate"] = final_df["Date"].apply(parse_date_for_sorting)
    final_df["SortTime"] = final_df["Time"].apply(
        lambda t: datetime.strptime(t, "%I:%M %p").time() if isinstance(t, str) else None
    )

    final_df = final_df.sort_values(
        by=["SortDate", "SortTime", "Home Team Name"],
        ascending=[True, True, True],
        kind="mergesort"
    )

    final_df = final_df.drop(columns=["SortDate", "SortTime"], errors="ignore")

    # Drop raw SSSL parsing columns
    drop_cols = [
        "Visitor", "Home", "HOLA_Team", "HOLA_Gender",
        "HOLA_AgeCode", "HOLA_AgeGroup", "HOLA_Division",
        "HOLA_Coach", "End",
    ]
    final_df = final_df.drop(columns=drop_cols, errors="ignore")

    # Reorder columns
    required_cols = [
        "Division Name", "Date", "Time",
        "Schedule Name", "Home Team Name", "Away Team Name",
    ]

    existing_required = [c for c in required_cols if c in final_df.columns]

    for c in required_cols:
        if c not in final_df.columns:
            final_df[c] = ""

    remaining_cols = [c for c in final_df.columns if c not in required_cols]

    final_df = final_df[existing_required + remaining_cols]

    # Competing Towns sheet
    competing_towns = (
        final_df.loc[final_df["Is_HOLA"] == True, "Town Competitors"]
        .dropna()
        .replace("", pd.NA)
        .dropna()
        .unique()
    )
    competing_df = pd.DataFrame({"Town Competitors": sorted(competing_towns)})

    print("Final columns:", final_df.columns.tolist())

    # -----------------------------
    # Write Excel (inside main!)
    # -----------------------------
    output_dir = "data/sssl"
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, "SSSL_Spring_2026_Schedule.xlsx")

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        final_df.to_excel(writer, sheet_name="Full Schedule", index=False)
        competing_df.to_excel(writer, sheet_name="Competing Towns", index=False)

    print(f"\nWrote schedule to {output_path}")


# ← MUST BE AT LEFT MARGIN
if __name__ == "__main__":
    main()
