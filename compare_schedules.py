import pandas as pd
from pathlib import Path

# -----------------------------
# Paths (repo-relative)
# -----------------------------
REPO_ROOT = Path(__file__).resolve().parents[0]

SSSL_FILE = REPO_ROOT / "data" / "sssl" / "SSSL_Spring_2026_Schedule.xlsx"
TS_FILE   = REPO_ROOT / "data" / "haysa" / "HAYSA_Spring_2026_Schedule.xlsx"
OUTPUT_FILE = REPO_ROOT / "data" / "merged" / "Schedule_Comparison_2026.xlsx"

SSSL_SHEET = "SSSL Schedule"
TS_SHEET   = "HAYSA Schedule"

HAYSA_TAG = "HOLA"   # how HAYSA teams appear in SSSL


# -----------------------------
# Helpers
# -----------------------------
def normalize_str(x):
    if not isinstance(x, str):
        return ""
    return " ".join(x.strip().split())


def build_key(df, date_col, time_col, field_col, home_col, away_col):
    """Build a normalized comparison key."""
    return (
        df[date_col].astype(str).apply(normalize_str)
        + " | "
        + df[time_col].astype(str).apply(normalize_str)
        + " | "
        + df[field_col].astype(str).apply(normalize_str)
        + " | "
        + df[home_col].astype(str).apply(normalize_str)
        + " | "
        + df[away_col].astype(str).apply(normalize_str)
    )


# -----------------------------
# Load Data
# -----------------------------
print(f"Loading SSSL schedule from {SSSL_FILE} ...")
sssl_df = pd.read_excel(SSSL_FILE, sheet_name=SSSL_SHEET)

print(f"Loading HAYSA schedule from {TS_FILE} ...")
ts_df = pd.read_excel(TS_FILE, sheet_name=TS_SHEET)


# -----------------------------
# Filter SSSL to HAYSA games only
# -----------------------------
sssl_haysa = sssl_df[
    sssl_df["Visitor"].astype(str).str.contains(HAYSA_TAG, na=False)
    | sssl_df["Home"].astype(str).str.contains(HAYSA_TAG, na=False)
].copy()


# -----------------------------
# Build comparison keys
# -----------------------------
# SSSL uses "Schedule Name" (TS field code)
sssl_haysa["Key"] = build_key(
    sssl_haysa,
    date_col="Date",
    time_col="Time",
    field_col="Schedule Name",
    home_col="Home",
    away_col="Visitor",
)

# TS scraper uses "Location" as the TS field code
ts_df["Key"] = build_key(
    ts_df,
    date_col="Date",
    time_col="Time",
    field_col="Location",   # <-- correct for your TS scraper
    home_col="Home",
    away_col="Away",        # <-- TS uses "Away", not "Visitor"
)


# -----------------------------
# Compare
# -----------------------------
sssl_keys = set(sssl_haysa["Key"])
ts_keys = set(ts_df["Key"])

sssl_only_keys = sssl_keys - ts_keys
ts_only_keys = ts_keys - sssl_keys

sssl_only = sssl_haysa[sssl_haysa["Key"].isin(sssl_only_keys)].copy()
ts_only = ts_df[ts_df["Key"].isin(ts_only_keys)].copy()

# Put Key first
if not sssl_only.empty:
    cols = ["Key"] + [c for c in sssl_only.columns if c != "Key"]
    sssl_only = sssl_only[cols]

if not ts_only.empty:
    cols = ["Key"] + [c for c in ts_only.columns if c != "Key"]
    ts_only = ts_only[cols]


# -----------------------------
# Save Comparison Report
# -----------------------------
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

with pd.ExcelWriter(OUTPUT_FILE) as writer:
    sssl_haysa.to_excel(writer, index=False, sheet_name="SSSL HAYSA Games")
    ts_df.to_excel(writer, index=False, sheet_name="TS Games")
    sssl_only.to_excel(writer, index=False, sheet_name="SSSL Only")
    ts_only.to_excel(writer, index=False, sheet_name="TS Only")

print(f"\nComparison complete. Saved to {OUTPUT_FILE}")
print(f"  SSSL-only games: {len(sssl_only)}")
print(f"  TS-only games:   {len(ts_only)}")
