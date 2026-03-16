import pandas as pd

# -----------------------------
# Config
# -----------------------------
SSSL_FILE = "SSSL_Spring_2026_Schedule.xlsx"
SSSL_SHEET = "SSSL Schedule"

TS_FILE = "HAYSA_Spring_2026_Schedule.xlsx"
TS_SHEET = "HAYSA Schedule"   # adjust if your TS sheet is named differently

OUTPUT_FILE = "Schedule_Comparison_2026.xlsx"

HAYSA_TAG = "HOLA"  # how HAYSA teams are tagged in SSSL


# -----------------------------
# Helpers
# -----------------------------
def normalize_str(x):
    if not isinstance(x, str):
        return ""
    return " ".join(x.strip().split())


def build_key(df, date_col, time_col, field_col, home_col, away_col):
    """
    Build a comparison key:
    Date | Time | Field | Home | Visitor
    All normalized to avoid whitespace issues.
    """
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
print("Loading SSSL schedule...")
sssl_df = pd.read_excel(SSSL_FILE, sheet_name=SSSL_SHEET)

print("Loading TS schedule...")
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
# Assumptions:
#  - SSSL: Date, Time, Schedule Name, Home, Visitor
#  - TS:   Date, Time, Field (TS code), Home, Visitor
# Adjust TS column names here if needed.
sssl_haysa["Key"] = build_key(
    sssl_haysa,
    date_col="Date",
    time_col="Time",
    field_col="Schedule Name",
    home_col="Home",
    away_col="Visitor",
)

ts_df["Key"] = build_key(
    ts_df,
    date_col="Date",
    time_col="Time",
    field_col="Field",      # <-- change if your TS field column is named differently
    home_col="Home",
    away_col="Visitor",
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

# For readability, put the key at the front
if not sssl_only.empty:
    cols = ["Key"] + [c for c in sssl_only.columns if c != "Key"]
    sssl_only = sssl_only[cols]

if not ts_only.empty:
    cols = ["Key"] + [c for c in ts_only.columns if c != "Key"]
    ts_only = ts_only[cols]

# -----------------------------
# Save Comparison Report
# -----------------------------
with pd.ExcelWriter(OUTPUT_FILE) as writer:
    sssl_haysa.to_excel(writer, index=False, sheet_name="SSSL HAYSA Games")
    ts_df.to_excel(writer, index=False, sheet_name="TS Games")
    sssl_only.to_excel(writer, index=False, sheet_name="SSSL Only")
    ts_only.to_excel(writer, index=False, sheet_name="TS Only")

print(f"\nComparison complete. Saved to {OUTPUT_FILE}")
print(f"  SSSL-only games: {len(sssl_only)}")
print(f"  TS-only games:   {len(ts_only)}")
